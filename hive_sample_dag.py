# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import textwrap
import uuid
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook

from airflow import DAG  # noqa
from airflow import macros  # noqa
from airflow.operators.python_operator import PythonOperator  # noqa
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.models.table_metadata import DESCRIPTION_NODE_LABEL
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


dag_args = {
    'concurrency': 10,
    # One dagrun at a time
    'max_active_runs': 1,
    # 4AM, 4PM PST
    'schedule_interval': '0 11 * * *',
    'catchup': False
}

default_args = {
    'owner': 'amundsen',
    'start_date': datetime(2020, 11, 18),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'priority_weight': 10,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120)
}

def get_neo4j_password():
    connection = BaseHook.get_connection('neo4j-test')
    return connection.password
    
# NEO4J cluster endpoints
NEO4J_ENDPOINT = 'bolt://neo4j:7687'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = get_neo4j_password()  #'test'


es_host = 'amundsen-elasticsearch-client'

es = Elasticsearch([
    {'host': es_host},
])

# Todo: user provides a list of schema for indexing
SUPPORTED_HIVE_SCHEMAS = ['product']
# Global used in all Hive metastore queries.
# String format - ('schema1', schema2', .... 'schemaN')
SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_HIVE_SCHEMAS))

# Todo: user needs to modify and provide a hivemetastore connection string
def connection_string():
    connection = BaseHook.get_connection('mysql-foo')
    return f'mysql+mysqldb://root:{connection.password}@mysql-foo:3306/metastore'


def create_table_wm_job(**kwargs):
    sql = textwrap.dedent("""
        SELECT From_unixtime(min(A0.create_time)) as create_time,
               'hive'                        as `database`,
               C0.NAME                       as `schema`,
               B0.tbl_name as table_name,
               {func}(A0.part_name) as part_name,
               {watermark} as part_type
        FROM   PARTITIONS A0
               LEFT OUTER JOIN TBLS B0
                            ON A0.tbl_id = B0.tbl_id
               LEFT OUTER JOIN DBS C0
                            ON B0.db_id = C0.db_id
        WHERE  C0.NAME IN {schemas}
               AND B0.tbl_type IN ( 'EXTERNAL_TABLE', 'MANAGED_TABLE' )
               AND A0.PART_NAME NOT LIKE '%%__HIVE_DEFAULT_PARTITION__%%'
        GROUP  BY C0.NAME, B0.tbl_name
        ORDER by create_time desc
    """).format(func=kwargs['templates_dict'].get('agg_func'),
                watermark=kwargs['templates_dict'].get('watermark_type'),
                schemas=SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE)

    logging.info('SQL query: {}'.format(sql))
    tmp_folder = '/var/tmp/amundsen/table_{hwm}'.format(hwm=kwargs['templates_dict']
                                                        .get('watermark_type').strip("\""))
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    hwm_extractor = SQLAlchemyExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=hwm_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
        'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.EXTRACT_SQL): sql,
        'extractor.sqlalchemy.model_class': 'databuilder.models.watermark.Watermark',
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # TO-DO unique tag must be added
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    job.launch()


def create_table_metadata_databuilder_job():
    """
    Launches data builder job that extracts table and column metadata from MySQL Hive metastore database,
    and publishes to Neo4j.
    @param kwargs:
    @return:
    """

    # Adding to where clause to scope schema, filter out temp tables which start with numbers and views
    where_clause_suffix = textwrap.dedent("""
        WHERE d.NAME IN {schemas}
        AND t.TBL_NAME NOT REGEXP '^[0-9]+'
        AND t.TBL_TYPE IN ( 'EXTERNAL_TABLE', 'MANAGED_TABLE' )
    """).format(schemas=SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE)

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

    job_config = ConfigFactory.from_dict({
        'extractor.hive_table_metadata.{}'.format(HiveTableMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY):
            where_clause_suffix,
        'extractor.hive_table_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
            connection_string(),
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_CREATE_ONLY_NODES):
            [DESCRIPTION_NODE_LABEL],
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # TO-DO unique tag must be added
    })

    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=HiveTableMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    job.launch()

def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   elasticsearch_doc_type_key='table',
                                   model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                   cypher_query=None,
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_search_index`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                       it uses the `Table` query baked into the Extractor
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): neo4j_user,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): neo4j_password,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            elasticsearch_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
            elasticsearch_doc_type_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
            elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if cypher_query:
        job_config.put('extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY),
                       cypher_query)
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    job.launch()



with DAG('amundsen_databuilder', default_args=default_args, **dag_args) as dag:

    amundsen_databuilder_table_metadata_job = PythonOperator(
        task_id='amundsen_databuilder_table_metadata_job',
        python_callable=create_table_metadata_databuilder_job
    )

    # calculate hive high watermark
    amundsen_hwm_job = PythonOperator(
        task_id='amundsen_hwm_job',
        python_callable=create_table_wm_job,
        provide_context=True,
        templates_dict={'agg_func': 'max',
                        'watermark_type': '"high_watermark"',
                        'part_regex': '{}'.format('{{ ds }}')}
    )

    # calculate hive low watermark
    amundsen_lwm_job = PythonOperator(
        task_id='amundsen_lwm_job',
        python_callable=create_table_wm_job,
        provide_context=True,
        templates_dict={'agg_func': 'min',
                        'watermark_type': '"low_watermark"',
                        'part_regex': '{}'.format('{{ ds }}')}
    )

    amundsen_es_job = PythonOperator(
        task_id='amundsen_es_job',
        python_callable=create_es_publisher_sample_job
    )

    # Schedule high and low watermark task after metadata task
    amundsen_databuilder_table_metadata_job >> amundsen_hwm_job >> amundsen_es_job
    amundsen_databuilder_table_metadata_job >> amundsen_lwm_job >> amundsen_es_job

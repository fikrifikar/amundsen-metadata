import logging
import os
import sqlite3

from pyhocon import ConfigFactory

from databuilder.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

logging.basicConfig(level=logging.INFO)

# set env NEO4J_HOST to override localhost
NEO4J_ENDPOINT = f'bolt://{os.getenv("NEO4J_HOST", "localhost")}:7687'
neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

es_host = 'local_host'
neo_host = 'local_host'


es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)


#NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port) #neo4j graphical interface uses bolt protocol
##default docker image sets uname and password as below
#neo4j_user = 'neo4j' 
#neo4j_password = 'test'

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception:
        logging.exception('exception')
    return None

def create_table_extract_job(**kwargs):

       #define path where both .csv files of nodes and relationships are defined where they are later saved in neo4j db
       tmp_folder = '/var/tmp/amundsen/{metadata_type}'.format(metadata_type=kwargs['metadata_type'])
       node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
       relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)
       
       bq_meta_extractor = BigQueryMetadataExtractor()
       csv_loader = FsNeo4jCSVLoader()
       task = DefaultTask(extractor=bq_meta_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
       
       job_config = ConfigFactory.from_dict({
           'extractor.bigquery_table_metadata.{}'.format(BigQueryMetadataExtractor.alami-group-data):
               kwargs['alami-group-data'], #project name
           'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
               node_files_folder,
           'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
               relationship_files_folder,
           'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
               True,
           'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
               node_files_folder,
           'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
               relationship_files_folder,
           'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
               NEO4J_ENDPOINT,
           'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
               neo4j_user,
           'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
               neo4j_password,
           'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
               'unique_tag',  # should use unique tag here like {bq_source}
       })
       
       #You can optionally set a label filter,
       #if you only want to pull tables with a certain label
       #label_filter should be in the following format: 'labels.key:value'
       if label_filter:
           job_config[
               'extractor.bigquery_table_metadata.{}'
               .format(BigQueryMetadataExtractor.FILTER_KEY)
               ] = label_filter
       
           job = DefaultJob(conf=job_config,
                       task=task,
                       publisher=Neo4jCsvPublisher())
       
       job.launch()
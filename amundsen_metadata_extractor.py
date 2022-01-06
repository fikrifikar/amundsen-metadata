neo_host = '35.219.42.200'
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7474) #amundsen default neo4j db server port, override env variable with target ip

NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port) #neo4j graphical interface uses bolt protocol
#default docker image sets uname and password as below
neo4j_user = 'neo4j' 
neo4j_password = 'test'

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
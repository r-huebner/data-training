import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('src/sparkify_redshift/dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

def get_table_schema_map():
    """
    Returns a dictionary that maps each table name to its SQL schema definition.
    """
    return {
        'staging_events':"""
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
        """,
    'staging_songs':"""
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
        """,
    # Fact table
    'songplay': """
        songplay_id INT IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    """,
    # Dimension table
    '"user"': """
        user_id INT,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
    """,
    # Dimension table
    'song': """
        song_id VARCHAR,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT
    """,
    # Dimension table
    'artist': """
        artist_id VARCHAR,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    """,
    # Dimension table
    '"time"': """
        start_time TIMESTAMP,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday BOOL
    """
    }

def get_table_key_map():
    """
    Returns a dictionary that maps table names to their distribution and sort keys.
    """
    return {
        'songplay': {'distkey': 'song_id', 'sortkey': 'start_time'},
        'song': {'distkey': 'song_id'},
        '"time"': {'sortkey': 'start_time'},
    }

def get_table_type_map():
    """
    Returns a dictionary that maps table names to their type:
    'staging', 'fact', or 'dimension'.
    """
    return {
        'staging_events': 'staging',
        'staging_songs': 'staging',
        'songplay': 'fact',
        '"user"': 'dimension',
        'song': 'dimension',
        'artist': 'dimension',
        '"time"': 'dimension'
    }

def get_column_source_map():
    """
    Returns a mapping of final star schema tables to their respective
    columns from the staging tables.
    """
    timestamp = "staging_events.ts / 1000 * interval '1 second' + TIMESTAMP '1970-01-01 00:00:00'"
    return {
      'songplay': {
          'start_time': timestamp,
          'user_id': 'staging_events.userId',
          'level': 'staging_events.level',
          'song_id': 'staging_songs.song_id',
          'artist_id': 'staging_songs.artist_id',
          'session_id': 'staging_events.sessionId',
          'location': 'staging_events.location',
          'user_agent': 'staging_events.userAgent'
      },
      '"user"': {
          'user_id': 'staging_events.userId',
          'first_name': 'MAX( staging_events.firstName )',
          'last_name': 'MAX( staging_events.lastName )',
          'gender': 'MAX( staging_events.gender )',
          'level': "MAX(CASE WHEN staging_events.level = 'paid' THEN 'paid' ELSE 'free' END)"
      },
      'song': {
          'song_id': 'staging_songs.song_id',
          'title': 'staging_songs.title',
          'artist_id': 'staging_songs.artist_id',
          'year': 'staging_songs.year',
          'duration': 'staging_songs.duration'
      },
      'artist': {
          'artist_id': 'staging_songs.artist_id',
          'name': 'MAX( staging_songs.artist_name)',
          'location': 'MAX( staging_songs.artist_location)',
          'latitude': 'MAX( staging_songs.artist_latitude)',
          'longitude': 'MAX( staging_songs.artist_longitude)'
      },
      '"time"': {
          'start_time': timestamp,
          'hour': f'EXTRACT(hour FROM {timestamp})',
          'day': f'EXTRACT(day FROM {timestamp})',
          'week': f'EXTRACT(week FROM {timestamp})',
          'month': f'EXTRACT(month FROM {timestamp})',
          'year': f'EXTRACT(year FROM {timestamp})',
          'weekday': f'CASE WHEN EXTRACT(dow FROM {timestamp}) IN (0, 6) THEN FALSE ELSE TRUE END'
      }
    }

def get_join_conditions():
    """
    Returns a dictionary defining join conditions for staging tables,
    used when multiple staging tables are joined to populate star schema tables.
    """
    return {
        'staging_songs': 'staging_events.song = staging_songs.title',
        'staging_events': 'staging_events.song = staging_songs.title'
    }

def get_where_constraints():
    """
    Returns a dictionary defining WHERE constraints for staging tables,
    used when filtering data before inserting into the star schema tables.

    Returns:
        dict[str, str]: Mapping of table names to SQL WHERE clauses.
    """
    return {
        'staging_events': "staging_events.page = 'NextSong' AND staging_events.userId IS NOT NULL",
        'staging_songs': "staging_songs.song_id IS NOT NULL"
    }

def get_groupby_cols():
    """
    Returns a dictionary defining the columns to be used in the GROUP BY clause for each table.
    This is used for deduplication or aggregation when inserting data into the star schema tables.

    Returns:
        dict[str, str]: Mapping of table names to the column(s) used for grouping.
    """
    return {
        '"user"': "user_id",
        'artist': "artist_id"
    }

def build_drop_queries(schema_map):
    """
    Builds SQL DROP TABLE IF EXISTS queries for all tables defined in the schema map.

    Parameters:
    - schema_map (dict): A mapping of table names to their SQL schema definitions.

    Returns:
    -  List[str]: A list of SQL DROP TABLE statements.
    """
    return [f"DROP TABLE IF EXISTS {table_name};" for table_name in schema_map.keys()]


def build_create_table_queries(schema_map, key_map):
    """
    Builds CREATE TABLE SQL statements for each table in the schema map.
    
    Parameters:
        schema_map (dict): Dictionary with table names as keys and SQL schema strings as values.

    Returns:
        List[str]: List of CREATE TABLE statements.
    """
    queries = []
    for table_name, schema in schema_map.items():
        dist_key = ""
        sort_key = ""
        keys = key_map.get(table_name)
        if keys:
            dist_key = f"DISTKEY({keys.get('distkey')})" if keys.get('distkey') else ""
            sort_key = f"SORTKEY({keys.get('sortkey')})" if keys.get('sortkey') else ""

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema}) {dist_key} {sort_key};"
        queries.append(query)

    return queries

def build_insert_table_queries(schema_map, type_map, source_map, join_map, where_map,groupby_map):
    """
    Builds INSERT INTO ... SELECT ... queries for populating star schema tables
    from staging data.

    Parameters:
        schema_map (dict): Table schema definitions.
        type_map (dict): Table type classification.
        source_map (dict): Mapping of columns from staging to dimensional tables.
        join_map (dict): Join conditions between staging tables.

    Returns:
        List[str]: List of INSERT INTO queries.
    """
    queries = []
    for table_name, schema in schema_map.items():
        if table_name not in source_map:
            continue
        
        # Get information from source mapping
        insert_columns = ",".join(source_map[table_name].keys())
        select_values = ",".join([f"{value} AS {key}" for key, value in source_map[table_name].items()])
        source_tables_tmp = list(set(val.split('.')[0] for val in source_map[table_name].values()))
        source_tables = list(set([val.split(' ')[-1] for val in source_tables_tmp]))

        # Get distinct source tables entries for dim tables to deduplicate
        distinct = "" if type_map[table_name] == "fact" else "DISTINCT"

        # FROM clause starts with the first source table
        from_clause = f"FROM {source_tables[0]}"

        # Add JOINs for other source tables if present
        for join_table in source_tables[1:]:
            join_condition = join_map.get(join_table)
            from_clause += f" JOIN {join_table} ON {join_condition}"

        # Build WHERE clause from all available constraints (no where-clause also possible)
        where_conditions = [where_map[table] for table in source_tables if table in where_map]
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

        # Build GROUPBY clause from all available constraints (no groupby-clause also possible)
        groupby_cols = groupby_map.get(table_name)
        groupby_clause = f"GROUP BY {groupby_cols}" if groupby_cols else ""

        query = f"""
            INSERT INTO {table_name} ({insert_columns}) 
            SELECT {distinct} {select_values}
            {from_clause}
            {where_clause}
            {groupby_clause};
            """

        queries.append(query)
        
    return queries


def build_copy_queries():
    """
    Builds COPY statements to load raw JSON data from S3 into staging tables.

    Returns:
        List[str]: COPY commands for all staging tables.
    """
    return [
        
        # Song JSON files -> Staging song table 
        f"""
        COPY staging_songs
        FROM '{SONG_DATA}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS JSON 'auto'
        REGION 'us-west-2';
        """,
        # Event JSON files -> Staging event table 
        f"""
        COPY staging_events
        FROM '{LOG_DATA}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS JSON '{LOG_JSONPATH}'
        REGION 'us-west-2';
        """
    ]

def build_sample_queries():
    """
    Returns a dictionary of sample analytical SQL queries on the star schema.

    Returns:
        dict[str, str]: Mapping of query descriptions to SQL SELECT statements.
    """
    return {
        "1. Get the 3 most popular songs by gender.": 
        """
            SELECT *
            FROM (
                SELECT 
                    ROW_NUMBER() OVER (PARTITION BY u.gender ORDER BY COUNT(*) DESC) AS rank,
                    u.gender, 
                    s.title, 
                    a.name AS artist, 
                    COUNT(*) AS play_count
                FROM songplay sp
                JOIN song s ON sp.song_id = s.song_id
                JOIN artist a ON sp.artist_id = a.artist_id
                JOIN "user" u ON sp.user_id = u.user_id 
                GROUP BY u.gender, s.title, artist
            ) sub
            WHERE rank <= 3;
        """,

        "2. Get the ten most played artists in 2018.": 
        """
            SELECT a.name, count(*) as count
            FROM songplay sp
            JOIN artist a ON sp.artist_id = a.artist_id
            JOIN "time" t ON sp.start_time = t.start_time
            WHERE t.year = 2018
            GROUP BY a.name
            ORDER BY count DESC
            LIMIT 10;
        """,

        "3. Get the 3 top users with the most played songs.": 
        """
            SELECT u.first_name || ' ' || u.last_name as name, count(*) as count
            FROM songplay sp
            JOIN "user" u ON sp.user_id = u.user_id 
            GROUP BY name
            ORDER BY count DESC
            LIMIT 3;
        """,

        "4. Get song count by location and level.": 
        """
            SELECT sp.location, sp.level, count(*) as count
            FROM songplay sp
            GROUP BY sp.location, sp.level
            ORDER BY sp.location;
        """
    }

def generate_row_count_check(table):
    return {
        'description': f'{table} is not empty',
        'sql': f'SELECT COUNT(*) FROM "{table}";',
        'expected_result': lambda x: x > 0
    }

def generate_null_check(table, column):
    return {
        'description': f'{table}.{column} has no NULL values',
        'sql': f'SELECT COUNT(*) FROM "{table}" WHERE {column} IS NULL;',
        'expected_result': lambda x: x == 0
    }

def generate_unique_check(table, column):
    return {
        'description': f'{table}.{column} is unique',
        'sql': f'SELECT COUNT({column}) - COUNT(DISTINCT {column}) FROM "{table}";',
        'expected_result': lambda x: x == 0
    }

def build_check_queries():
    queries=[]

    primary_keys = {
        'user': 'user_id',
        'artist': 'artist_id',
        'song': 'song_id',
        'time': 'start_time',
        'songplay': 'songplay_id'
    }

    # Check PKs
    for table, pk in primary_keys.items():
        queries.append(generate_row_count_check(table))
        queries.append(generate_null_check(table, pk))
        queries.append(generate_unique_check(table, pk))

    # Additional checks
    queries.append(generate_null_check('songplay', 'user_id'))
    queries.append(generate_null_check('songplay', 'start_time'))

    return queries

def get_all_queries():
    """
    Constructs all SQL queries needed for the ETL pipeline.

    Returns:
        dict: A dictionary with keys:
            - 'drop': DROP TABLE IF EXISTS statements
            - 'create': CREATE TABLE statements
            - 'insert': INSERT INTO SELECT statements
            - 'copy': COPY FROM S3 statements
            - 'sample': sample SELECT queries
              
    """
    schema_map = get_table_schema_map()
    key_map = get_table_key_map()
    type_map = get_table_type_map()
    source_map = get_column_source_map()
    join_map = get_join_conditions()
    where_map = get_where_constraints()
    groupby_map = get_groupby_cols()

    drop_queries = build_drop_queries(schema_map)
    create_queries = build_create_table_queries(schema_map, key_map)
    insert_queries = build_insert_table_queries(schema_map, type_map, source_map, join_map, where_map,groupby_map)
    copy_queries = build_copy_queries()
    sample_queries = build_sample_queries()
    check_queries = build_check_queries()
    
    return {
        'drop': drop_queries,
        'create': create_queries,
        'insert': insert_queries,
        'copy': copy_queries,
        'sample': sample_queries,
        'check': check_queries
    }

# Generate all queries
queries = get_all_queries()
drop_table_queries = queries['drop']
create_table_queries = queries['create']
insert_table_queries = queries['insert']
copy_table_queries = queries['copy']
sample_queries = queries['sample']
check_queries = queries['check']



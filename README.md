# Demo of How Greenplum Database Can Load and Process HL7 Data

HL7 is a specification for exchanging clinical health care data.  The specification has
been in existence for about 30 years, so there is likely an abundance of data in this format.
The format itself isn't very intuitive so, when we looked at it, converting it to a more
up to date format seemed attractive.  JSON was our choice here, and Greenplum Database
now provides a JSON data type with operators for querying JSON data.  Below, we explore
how one might load and operate on one of these HL7 data sets.

Note that, here, we load the data from the Greenplum master node only.  In a higher data volume
scenario, we would have chosen to distribute the work across all the segments in our GPDB
cluster.  We'd be happy to provide more detail on that, so please ask if you are interested.

## Preparation

1. Data Generation: we generated some HL7 formatted data in a file in /tmp/ on our GPDB master node:
    ```
    OBX|1|TX|1001^Reason For Visit: |1|Evaluated patient for restrictive_dieting. Drew blood. ||||||F
    OBX|2|TX|1002^Reason For Visit: |2|Started treatment for loss_of_coordination. ||||||FAL|AL|AUS
    OBX|3|TX|1003^Reason For Visit: |3|Started treatment for difficulty_breathing. Discharged. ||||||F
    OBX|4|TX|1004^Reason For Visit: |4|The patient complained of weight_gain.  Performed tests. ||||||F
    OBX|5|TX|1005^Reason For Visit: |5|Evaluated patient for body_pains.  Performed tests. ||||||FS
    OBX|6|TX|1006^Reason For Visit: |6|Reason for visit was skin_cracking. Drew blood. ||||||FL|AUS
    OBX|7|TX|1007^Reason For Visit: |7|Visited me because of prolonged_bleeding. Discussed changes. ||||||F
    ...
    ```

1. First, we wrote a Python HL7 to JSON transformer, to pipe our input data through:

    ```python
    """Converting HL7 messages to dictionaries
    # http://www.prschmid.com/2016/11/converting-adt-hl7-message-to-json.html

    Example Usage:
        import pprint
        from hl7apy.parser import parse_message
        # Taken from http://hl7apy.org/tutorial/index.html#elements-manipulation
        s = "MSH|^~\&|GHH_ADT||||20080115153000||ADT^A01^ADT_A01|0123456789|P|2.5||||AL ... "
        pprint.pprint(hl7_str_to_dict(s))

    """

    import codecs
    from sys import stdin
    from io import TextIOWrapper, BufferedReader, BytesIO
    from pprint import pprint
    from json import dumps
    from hl7apy.parser import parse_message
    from hl7apy.exceptions import ParserError

    def hl7_str_to_dict(s, use_long_name=True):
        """Convert an HL7 string to a dictionary
        :param s: The input HL7 string
        :param use_long_name: Whether or not to user the long names
                              (e.g. "patient_name" instead of "pid_5")
        :returns: A dictionary representation of the HL7 message
        """
        #s = s.replace("\n", "\r")
        #print(s)
        try:
            m = parse_message(s)
            return hl7_message_to_dict(m, use_long_name=use_long_name)
        except ParserError:
            return dict() 

    def hl7_message_to_dict(m, use_long_name=True):
        """Convert an HL7 message to a dictionary
        :param m: The HL7 message as returned by :func:`hl7apy.parser.parse_message`
        :param use_long_name: Whether or not to user the long names
                              (e.g. "patient_name" instead of "pid_5")
        :returns: A dictionary representation of the HL7 message
        """
        if m.children:
            d = {}
            for c in m.children:
                name = str(c.name).lower()
                if use_long_name:
                    name = str(c.long_name).lower() if c.long_name else name
                dictified = hl7_message_to_dict(c, use_long_name=use_long_name)
                if name in d:
                    if not isinstance(d[name], list):
                        d[name] = [d[name]]
                    d[name].append(dictified)
                else:
                    d[name] = dictified
            return d
        else:
            return m.to_er7() 

    for s in stdin.read().split("\n"):
        d = hl7_str_to_dict(s)
        print dumps(d)
    ```

1. Next, we built an external web table for use in transforming the HL7 data to JSON format as it was loaded:

    ```sql
    CREATE READABLE EXTERNAL WEB TABLE hl7_ext (
      jsonrecord JSON
    )
    EXECUTE 'cat /tmp/hl7-samples.txt | PYTHONIOENCODING="ISO-8859-1" python /home/gpadmin/hl7_to_dict.py' ON MASTER
    FORMAT 'TEXT' (DELIMITER 'OFF' NULL '\N' ESCAPE '\');
    ```

1. We then created a compressed, append-optimized "heap" table as we loaded the data:
    ```sql
    CREATE TABLE hl7
    WITH (appendonly=true, compresstype=zlib, compresslevel=5)
    AS SELECT * FROM hl7_ext
    DISTRIBUTED RANDOMLY;
    ```

## Querying the data

* Suppress any warnings or tips:
    ```
    SET client_min_messages to warning;
    ```

* What is the distribution of visits across the 24 hours in the day?
    ```sql
    SELECT DATE_PART('hour', (jsonrecord->'txa'->'activity_date_time'->'time_of_an_event'->>'st')::TIMESTAMP) AS hour
      , COUNT(*)
    FROM hl7_testing
    GROUP BY 1
    ORDER BY 1 ASC;

     hour | count 
    ------+-------
        2 | 50843
        3 |   128
    (2 rows)

    Time: 400.570 ms
    ```

* What are the top 10 most common birth years of patients in this data set?
    ```sql
    SELECT DATE_TRUNC('year', (jsonrecord->'pid'->'date_time_of_birth'->'time_of_an_event'->>'st')::date) AS "Birth Year"
      , COUNT(*)
    FROM hl7_testing
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10;

           Birth Year       | count 
    ------------------------+-------
     1936-01-01 00:00:00-05 |   720
     1949-01-01 00:00:00-05 |   688
     1923-01-01 00:00:00-05 |   688
     1946-01-01 00:00:00-05 |   678
     1955-01-01 00:00:00-05 |   675
     1960-01-01 00:00:00-05 |   673
     1927-01-01 00:00:00-05 |   671
     1935-01-01 00:00:00-05 |   669
     1939-01-01 00:00:00-05 |   667
     1953-01-01 00:00:00-05 |   667
    (10 rows)

    Time: 429.891 ms
    ```

* What are the top 10 most common words used in the `observation_value`?
    ```sql
    WITH words AS (
      SELECT REGEXP_SPLIT_TO_TABLE(LOWER(jsonrecord->'obx'->'observation_value'->'varies_1'->>'st'), E'\\W+') AS word
      FROM hl7_testing
    )
    SELECT word, COUNT(*) AS cnt
    FROM words
    WHERE LENGTH(word) > 0 AND word NOT IN (
      'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by','for', 'if', 'in', 'into', 'is', 'it','no', 'not', 'of',
      'on', 'or', 'such','that', 'the', 'their', 'then', 'there', 'these','they', 'this', 'to', 'was', 'will', 'with'
    )
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20;

        word    |  cnt  
    ------------+-------
     patient    | 20592
     evaluated  | 10316
     complained | 10277
     visited    | 10265
     because    | 10265
     me         | 10265
     started    | 10117
     treatment  | 10117
     visit      |  9996
     reason     |  9996
     tests      |  6452
     performed  |  6452
     hospital   |  6430
     admitted   |  6430
     medication |  6426
     prescribed |  6426
     blood      |  6395
     drew       |  6395
     changes    |  6323
     discussed  |  6323
    (20 rows)

    Time: 603.528 ms
    ```

## References

* An [article](http://clarkdave.net/2013/06/what-can-you-do-with-postgresql-and-json/) on JSON support in PostgreSQL
* [Documentation](https://gpdb.docs.pivotal.io/500/admin_guide/query/topics/json-data.html) on JSON support in Greenplum DB
* An interesting [article](https://www.fknsrs.biz/blog/golang-hl7-library.html.html) with background on the HL7 format


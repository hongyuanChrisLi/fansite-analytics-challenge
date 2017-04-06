# Table of Contents
1. [Project Summary](README.md#project-summary)
2. [Repo directory structure](README.md#repo-directory-structure)
3. [Tests](README.md#tests)


# Project Summary

This project aims to solve the challenges posted by the Insight Data Science program. 
https://github.com/InsightDataScience/fansite-analytics-challenge. 

Four required features are implemented, using PySpark, in order to build a scalable program. To run this program, place the log.txt file in `log_input/` , execute run.sh in the project root directory and output files are generated in `log_output/`

Script run.sh calls `src/process_log.py` which initializes the spark context and execute components for each feature, one by one. A Timer (module: `src/utility/timer`) is instantiated in `process_log.py` to time the execution of each component for performance study. 

Feature 1: top 10 hosts/IPs => module: `src/utility/util_hosts` 

Feature 2: top 10 resources => module: `src/utility/util_resources`

Feature 3: top 10 busiest 60-minute periods => module: `src/utility/util_hours`

Feature 4: blocked hosts/IPs => module: `src/utiliy/util_blocked`


### Feature 1: 
List the top 10 most active host/IP addresses that have accessed the site.

Call function `output_top_hosts` in `src/utility/util_hosts.py` 

    Input file rdd and write to output file hosts.txt
    1. map: extracts host names or IP addresses from input as the keys
    2. reduce: count by keys


### Feature 2: 
Identify the 10 resources that consume the most bandwidth on the site

Call function `output_top_resource` in `src/utility/util_resources.py` 

    Input file rdd and write to output file resources.txt
    1. map: extracts resource names and bytes from input as a key value pair
    2. reduce: adds up bytes by keys

### Feature 3:
List the top 10 busiest (or most frequently visited) 60-minute periods 

Call function `output_top_hours_tuned` in `src/utility/util_hours.py` 

In this function, first, timestamps are converted to offset seconds based on the first timestamp (the start) of the log file. Then, the whole data set is divided into buckets (2 hour length each) to identify narrower ranges where the top 10 busiest 60=min periods could occur. In the end, the top 10 60-minute periods are calculated within selected buckets. For 4 million records, this function takes about 3.5 minutes. It's a significant improvement upon a deprecated function `output_top_hour`.

Deprecated function `output_top_hour` doesn't have the bucket selection process, which generates 3600 * 4 million records in total, a waste of time and space. This function take about 1 hour to complete. 

I also wrote a serial processing class `serialHours` in `src/serial/serial_hours.py`. This is the old fashioned line-by-line computing, not scalable, but without the overheads of partitions and input splits. On a single machine, this method outperforms the distributed method. It takes about 1.5 minutes to complete. To run this script:

    python serial_hours.py

### Feature 4: 
Detect patterns of three failed login attempts from the same IP address over 20 seconds so that all further attempts to the site can be blocked for 5 minutes. Log those possible security breaches.

Call function `output_blocked_hosts` in `src/utility/util_blocked.py`

    Input file rdd and write to output file blocked.txt
    1. map: extracts resource, hosts/IPs, offset names and reply codes from input 
    2. filter: keeps values with resource name == '/login' 
    3. map: generate a new key-value pair where host/IP is the key 
         and the value is a list with one tuple (offset second, reply code, the original input string)
    4. reduce: combines all lists by host/IPs)
    5. map: for each host/IP, finds the blocked request attempts as a blocked list
    6. reduce: combines all blocked lists from all hosts/IPs


# Repo Directory Structure

The directory structure looks like this:

    ├── README.md 
    ├── run.sh
    ├── src
    │   └── process_log.py
        ├── utility
        │   └── file_writer.py
        │   └── timer.py
        │   └── util.py
        │   └── util_blocked.py
        │   └── util_hosts.py
        │   └── util_hours.py
        │   └── util_resources.py
        ├── serial
        │   └── serial_hours.py   
    ├── log_input
    │   └── log.txt
    ├── log_output
    |   └── hosts.txt
    |   └── hours.txt
    |   └── resources.txt
    |   └── blocked.txt
    ├── insight_testsuite
    |   └── run_tests.sh
    |   └── unit_tests.py 
    |   └── tests
            └── test_features
            |   ├── log_input
            |   │   └── log.txt
            |   |__ log_output
            |   │   └── hosts.txt
            |   │   └── hours.txt
            |   │   └── resources.txt
            |   │   └── blocked.txt
            ├── more_test_features
                ├── log_input
                │   └── your-own-log.txt
                |__ log_output
                |   └── hosts.txt
                |   └── hours.txt
                |   └── resources.txt
                |   └── blocked.txt

# Tests

To run integration tests, call `run_tests.sh` in the `insight_testsuite` folder.

To run unit tests, call `python unit_tests.py` in the `insight_testsuite` folder.


# Table of Contents
1. [Project Summary](README.md#project-summary)
2. [Details of Implementation](README.md#details-of-implementation)
3. [Repo directory structure](README.md#repo-directory-structure)


# Challenge Summary


### Feature 1: 
List the top 10 most active host/IP addresses that have accessed the site.

### Feature 2: 
Identify the 10 resources that consume the most bandwidth on the site

### Feature 3:
List the top 10 busiest (or most frequently visited) 60-minute periods 

### Feature 4: 
Detect patterns of three failed login attempts from the same IP address over 20 seconds so that all further attempts to the site can be blocked for 5 minutes. Log those possible security breaches.



## Repo directory structure

The directory structure for your repo should look like this:

    ├── README.md 
    ├── run.sh
    ├── src
    │   └── process_log.py
    ├── log_input
    │   └── log.txt
    ├── log_output
    |   └── hosts.txt
    |   └── hours.txt
    |   └── resources.txt
    |   └── blocked.txt
    ├── insight_testsuite
        └── run_tests.sh
        └── tests
            └── test_features
            |   ├── log_input
            |   │   └── log.txt
            |   |__ log_output
            |   │   └── hosts.txt
            |   │   └── hours.txt
            |   │   └── resources.txt
            |   │   └── blocked.txt
            ├── your-own-test
                ├── log_input
                │   └── your-own-log.txt
                |__ log_output
                    └── hosts.txt
                    └── hours.txt
                    └── resources.txt
                    └── blocked.txt

You simply clone this repo, but <b>please don't fork</b> it.
The contents of `src` do not have to contain a single file called `process_log.py`, you are free to include one or more files and name them as you wish.

## Testing your directory structure and output format

To make sure that your code has the correct directory structure and the format of the output files are correct, we included a test script, called `run_tests.sh` in the `insight_testsuite` folder.

The tests are stored simply as text files under the `insight_testsuite/tests` folder. Each test should have a separate folder and within should have a `log_input` folder for `log.txt` and a `log_output` folder for outputs corresponding to the current test.

You can run the test with the following from the `insight_testsuite` folder:

    insight_testsuite~$ ./run_tests.sh 


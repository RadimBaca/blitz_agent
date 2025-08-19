# Manual Tests

This directory contains test scripts that are meant to be run manually, not as part of the automated pytest suite.

## workload_generator_manual_test.py

A standalone script to run the workload generator. This script connects to a database and runs various workload generation functions.

**Usage:**
```bash
python init_adventure_works.py
python workload_generator_manual_test.py
```

**Note:** This script requires a properly configured database connection and should not be run as part of the automated test suite.

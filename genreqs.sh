#!/bin/bash
pip freeze | grep -v "^\-e" > requirements.txt

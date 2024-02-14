Overview
========

A Web Traffic Pipeline designed to ingest CSV files containing Web Traffic Data and perform necessary configurations, transformations and validations using factory objects.


Requirements
============

* Python 3.8+
  * requests==2.31.0 
  * PyYAML==6.0.1 
  * pandas==2.2.0

Getting Started
=======

Step 1 - Install requirements
``pip install -r requirements.txt``

Step 2  - Run script
``python main.py``

Architecture
=============

### Factories

The idea is that for every new ETL pipeline there is a factory equivalent in the form of a class object that provides context to the given pipeline.

The factory requires one argument at the point of instantiation, and that is the configuration file path (yaml). 

A configuration file must have the following keys in order to run: root_url, source_schema, and output_location.

In the given context of this project we have the Web Traffic factory


### Why Factories?

#### Modularization: 
All functions and attributes assigned to the factory have a designed purpose that is insulated from the rest of the object's attributes. It helps with organization, the identification of eventual bugs, as well as extensibility for new functionality.

#### Testing: 
Factory objects make it easier to use functionality that already has unit tests in place, identify what functionality requires unit testing(unit testing coverage %), and it also makes it much easier for further test-driven development of new functionality in the factory object. 

#### Resuability: 
The concept of a factories makes it much easier to reuse core functionality across new ETL development that can vary in subject/source system. This can help developers save time when it comes to using specific functionality that has already been developed and more importantly, unit tested. 

  
Brief Data Catalogue
=======
##### (This section could potentially be specified in the config file for each factory object)

### Source System Schema

`drop`: Whether or not this was the last page the user visited before leaving the site.

`length`: How long the user spent on the page in seconds.

`path`: The page within the website that the user visited.

`user_agent`: The browser identifier of the user visiting the page

`user_id`: The unique identifier for the user visiting the page.

### Data Lineage
Each file is retrieved via HTTP request to root url, subsequently transformed and stored locally in the configured output_location 

### Dependencies
`HTTP URL endpoint`: Request to https://public.wiwdata.com/engineering-challenge/data/

Performing Unit Tests
=======

Run in terminal, from inside project directory `python -m unittest tests/test_web_traffic_factory.py
`


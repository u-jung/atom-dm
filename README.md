
  
### :red_circle: This is still work in progress. Please wait until the main project has been released. Thanks ###

# atom-dm #

Welcome,

*atom-dm* stands for *Atom Data Manager*. It's a group of tools set up around AtoM.
AtoM stands for Access to Memory. It is a web-based, open source application for standards-based archival description and access in a multilingual, multi-repository environment. (see also <https://www.accesstomemory.org>)

The aim of atom-dm is to harvest data from different repositories, to prepare this data and to import them into AtoM database. It can also do some maintainance work inside the AtoM database.
It was created during a project on archives and German colonial past hosted by the Potsdam University of Applied Sciences (www.fh-potsdam.de)

One very interesting thing is the integration of data from Wikidata. These data are used to build a set of search terms for data import. They are also used for the maintainance of actor and term information inside AtoM.

## Suported sites for data import ##
### Archival descriptions ###
* DDB - www.deutsche-digitale-bibliothek.de / www.archivportal.de  (You need your API-Key to get access to their REST API)
* KAL - http://kalliope-verbund.info  (via SRU)
* FBN - Different repositories hosted on www.findbuch.net

### authority data ###
* Wikidata - www.wikidata.org (You have to adopt the SPARQL-Queries inside this code)
* Sigel-Verzeichnis  - http://sigel.staatsbibliothek-berlin.de

### The Wikidata impact ###
As the project is asking about traces of German Colonial Past in archives. 
It was necessary to create a main corpus of data objects related to this topic. 
We used "Deutsche Kolonien und Schutzgebiete" (https://www.wikidata.org/wiki/Q329618) as data root. You may test other topics. 
Here comes the query. You can use it at https://query.wikidata.org

```
SELECT DISTINCT ?item ?itemLabel ?itemDescription
	where 
	{
	{optional{?item wdt:P17/wdt:P279* wd:Q329618 .}}
	 union
	{optional{?item wdt:P2541/wdt:P279* wd:Q329618 .}}
	 union
	{ optional{?item wdt:P131/wdt:P279* wd:Q329618 .}} 
	 union
	{ optional{?item wdt:P17/wdt:P279* wd:Q329618 .}}
	 union
	{ optional{?item wdt:P361/wdt:P279* wd:Q329618 .}}
	 union
	{ optional{?item wdt:P2650/wdt:P279* wd:Q329618 .}}
	 union
	{ optional{?item wdt:P937/wdt:P279* wd:Q329618 .}}
	   SERVICE wikibase:label { bd:serviceParam wikibase:language "de,en,fr". }
	 } 
	order by ?item
```

## Installation ##
*atom-dm* has been created to running on a Ubuntu 16.4 server installation which host also a recommended AtoM 2.4 installation using nginx an mySQL.
Just unpack the zip file into the user's home directory. Once installed you can start the different tasks using the console.
Note: You need to enter your MySQL parameter and API-Keys into atom/config/mysql_opts.py and atom/config/api_key.py 

Try:

`./atom-dm {task group option} {more options}`

### What's already working ###
#### Filling the keyword list using a Wikidata Query ####
The retrieved keywords are stored in *atom/data/keywords.json* . To use your own query replace parts of teh query stored as WD_ENTITY_QUERY inside atom/main/data_manager.py (class: DataManager). Make sure that your query retrieves a column of Wikidata object URI called *?item*.

Try:

`./atom-dm --index-keywords`

#### Retrieving archival description from www.deutsche-digitale-bibliothek.de ####
Remember you need to store your API-Key inside  *atom/config/api_key.py*

Try:

`./atom-dm -i ddb`

The imported data are stored in chunks of around 100 records as *import.csv* subfolders of *atom/tmp_results*. 
At the end of teh process they will be all joined into *atom/tmp_results/import.csv*
This file is ready for import into AtoM. But you should verify the data before do so. 
Verification has two steps. 
* You need manually mark obsolete items (e.g. Looking for *Kolonie* my also give results as *Villenkolonie* or *Ferienkolonie*. See *Join data* for more information.
* Check for duplicate items and respected hierarchy.  A special file *import_preparation.log* for this along with import.csv.
Note! It is usefull to not differ the names of different import.csv files. AtoM use them as part of premary key for look up tasks.
During the task the tools checks also which one of the wikidata keywords will fit the retrieved items. 

#### Retrieving archival description from Kalliope-Verbund ####

Try:

`./atom-dm -i kal`

The procedure is the same as described on *Retrieving archival description from www.deutsche-digitale-bibliothek.de*

### Here comes the Query to get a list of libraries and archives described by Wikidata ###
(You can use it at https://query.wikidata.org)

```
SELECT distinct ?isil ?item ?itemLabel  WHERE {
  { ?item wdt:P31/wdt:P279* wd:Q166118. }
  UNION
  { ?item wdt:P31/wdt:P279* wd:Q7075. }
  optional {?item wdt:P791 ?isil}
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de,en, jp,it,es,fr,gr,ru,zh,dk,fi,nl" . } 
}
Order by ?isil
```


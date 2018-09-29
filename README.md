
  
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
DDB - www.deutsche-digitale-bibliothek.de / www.archivportal.de  (You need your API-Key to get access to their REST API)
KAL - http://kalliope-verbund.info  (via SRU)
FBN - Different repositories hosted on www.findbuch.net

### authority data ###
Wikidata - www.wikidata.org (You have to adopt the SPARQL-Queries inside this code)
Sigel-Verzeichnis  - http://sigel.staatsbibliothek-berlin.de


## Installation ##
*atom-dm* has been created to running on a Ubuntu 16.4 server installation which host also a recommended AtoM 2.4 installation using nginx an mySQL.
Just unpack the zip file into the user's home directory. Once installed you can start the different tasks using the console.
Note: You need to enter your MySQL parameter and API-Keys into atom/config/mysql_opts.py and atom/config/api_key.py 
`./atom-dm {task group option} {more options}`

### What's working already ###
#### Filling the keyword list using a Wikidata Query ####
The retrieved keywords are stored in *atom/data/keywords.json* . To use your own query replace parts of teh query stored as WD_ENTITY_QUERY inside atom/main/data_manager.py (class: DataManager). Make sure that your query retrieves a column of Wikidata object URI called *?item*.
`./atom-dm --index-keywords`

## Workflows ##
### Import of data ###

### Treatment of data ###


### Translation ###
It is possible to translate descriptions into other language using the service of www.DeepL.com



## The Wikidata impact ##
As the project is asking about traces of German Colonial Past in archives. It was necessary to create a main corpus of data objects related to this topic. We used "Deutsche Kolonien und Schutzgebiete" (https://www.wikidata.org/wiki/Q329618) as data root. You may test other topics. 
Here is comes the query. You can use it at https://query.wikidata.org

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
# atom-dm

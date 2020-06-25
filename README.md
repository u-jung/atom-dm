[https://github.com/u-jung/atom-dm]
  
### :red_circle: This should be still work in progress. But it has now become the final release as the project ended. Thanks (5.15.2019)###

# atom-dm #

Welcome,

*atom-dm* stands for *Atom Data Manager*. It's a group of tools which has been set up around AtoM.
AtoM stands for Access to Memory. It is a web-based, open source application for standards-based archival description and access in a multilingual, multi-repository environment. (see also <https://www.accesstomemory.org>)

The aim of *atom-dm* is to harvest data from different repositories, to prepare this data and to import them into AtoM database. It can also do some maintainance work inside the AtoM database.
It was created during a project on archives and German colonial past hosted by the Potsdam University of Applied Sciences (www.fh-potsdam.de)

One very interesting thing is the integration of data from Wikidata. These data are used to build a set of search terms for data import. They are also used for the maintainance of actor and term information inside AtoM.

This tool set has been created to support the Archive Guide to sources on German colonial past (<https://www.archivfuehrer-kolonialzeit.de>)


## Suported sites for data import ##
### Archival descriptions ###
* DDB - www.deutsche-digitale-bibliothek.de / www.archivportal.de  (You need your API-Key to get access to their REST API)
* KAL - http://kalliope-verbund.info  (via SRU)
* FBN - Different repositories hosted on www.findbuch.net
* EADXML - standalone EAD2002 XML Files

### Authority data ###
* Wikidata - www.wikidata.org (You have to adopt the SPARQL-Queries inside this code. Specially the main query stored at atom/main/g.py)
* Sigel-Verzeichnis  - http://sigel.staatsbibliothek-berlin.de

### The Wikidata impact ###
As the project is asking about traces of German Colonial Past in archives it was necessary to create a main corpus of data objects related to this topic. 
We used "Deutsche Kolonien und Schutzgebiete" (https://www.wikidata.org/wiki/Q329618) as data root. You may test other topics. 
Here comes the query. You can use it at <https://query.wikidata.org>

```
SELECT DISTINCT ?item ?itemLabel ?itemDescription where 
{ ?item (wdt:P17|wdt:P19|wdt:P20|wdt:P27|wdt:P36|wdt:P119|wdt:P131|wdt:P159|wdt:P180|wdt:P189|wdt:P276|wdt:P279|wdt:P291|wdt:P361|
	wdt:P551|wdt:P740|wdt:P915|wdt:P840|wdt:P921|wdt:P937|
	wdt:P1001|wdt:P1071|wdt:P1269|wdt:P1376|wdt:P1416|\
	wdt:P2341|wdt:P2541|wdt:P2647|wdt:P2650)/(wdt:P31*|wdt:P361*|wdt:P131*|wdt:P279*) wd:Q329618 . 
SERVICE wikibase:label { bd:serviceParam wikibase:language "de,fr,en". } } order by ?item
```

If you are using another corpus, please change the value of WD_ENTITY_QUERY in atom/main/g.py

## Installation ##
*atom-dm* has been created to running on a Ubuntu 16.4 server installation which host also a recommended AtoM 2.4 installation using nginx and mySQL.
Just unpack the zip file into the user's home directory. Once installed you can start the different tasks using the console.
Note: You need to enter your MySQL parameter and API-Keys into atom/config/mysql_opts.py and atom/config/api_key.py 

Go to the unpacked directory and try:

`./atom-dm {task group option} {more options}`

### Tools that are working ###

**Disclaimer**: This piece of work comes without any warranty. Please make sure you have always a backup copy of your data before trying out

#### Task group option####

Task group option can be:
`-i ` (for import tasks)
`-m ` (for maintenance tasks)
`-h ` (to show this help file as raw text)

### Import tasks###


#### The search term generator####
For import tasks a list of search terms will be generated from different sources. You can use the following options in combination with every import task:

If you want to pass smaller predefined list of search terms, use predefined argument followed by a comma seperated list of keywords. e.g.

`./atom-dm -i source={import option} predefined=Kamerun,Deutsch-Ostafrika,Samoa,Deutsch-Südwestafrika,Togo, Kiautchou,Deutsch-Neuguinea`

If `predefined=exclusive` the list of search terms stored as `PREDEFINED_SEARCH_TERMS` in *atom/main/g.py* wil be used. The option `predefined=` (without arguments) will use both the search term list stored as *atom/main/g.py* as well as the list in *atom/data/keywords.json*.

The option `without_frequent_names=True` is used to exclude frequent search terms. A frequent search term is a search term where each word is listed in the *file atom/data/frequent_names.txt* .  

Some termes are excluded because of their Wikidata description. You can define a list of such block words at `BLOCKED_BY_DESCRRIPTION` in */atom/main/g.py*

The option `last-revision=2019-01-01_00:00:00` makes sure that only search terms where used from *atom/data/keywords.json* if there were modified after the mentioned date. 

The option `from-term={term}` starts the search term generator from the mentioned word. The generator iterates in alphabetical order. So if `from-term=Kamerun` all search_term before Kamerun will be excluded. 

You can manually modify the keyword list by adding an `exclusions` key to the keyword which is populated with a list of excluded forms of the keyword (e.g. item Q33938 - Hausa). This will be necessary in case the search term key list of this items contains a noisy element (here "Hauses" - the genitive of german "Haus" (House)). The search term key list will be generated automatically by using all labels and alternative labels from the Wikidata object.

##### Retrieving archival description from www.deutsche-digitale-bibliothek.de #####
Remember you need to store your API-Key inside  *atom/config/api_key.py*
The process uses the keywords stored inside *atom/data/keywords.json* to lookup for items. To populate  the keyword file, see the chapter *Populate the keyword list using a Wikidata Query* below. 

Try:

`./atom-dm -i source=ddb`

You may also use one or more options from the previous chapter* The search term generator*.

#### Retrieving archival description from Kalliope-Verbund ####

Try:

`./atom-dm -i source=kal`

You may also use one or more options from the previous chapter* The search term generator*.

#### Retrieving archival description from a repository inside findbuch.net ####
**Important!** Before using this feature make sure you have the right to do so. Ask the concerned institution if you may use their metadata!

Befor you can start retrieving data from one of those repository open the site of the repository inside your browser. Now get the following informations:
* the sub domain of the repository aka the part between *https://*and *.findbuch.net* inside the url
* the repository id. You will find this as the arid parameter inside the url query string
* a valid PHP session id. Open the javascript console on your browser and enter *document.cokie* . You can use the session cookie with or without the `PHPSESSID=` as in the `session` option.  It is also possible to replace the current value of `PHP_SESSION_COOKIE` inside *atom/main/g.py* with your cookie parameter!

Try:

`./atom-dm -i source=fbn archive-url={repository sub domain} archive-id{repository id} session={PHP session id}`


You may also use one or more options from the previous chapter* The search term generator*.
*Another important note. * Findbuch.net allows the institution to label the fields using their own preferences. Some times the field label preferences changed over the years. As a result you may find  different labels for the same concept in different records. 
The most common field labels are already labeled. You will find them inside the constant `FIELD_MAPPING` in *atom/imports/findbuch.py* Check the online database of your prefered institution. If you find a label which is not already mapped you need to update the `FIELD_MAPPING` constant.


#### Retrieving archival description from an EAD-XML file ####

Try:

`./atom-dm -i source=eadxml sourcefile={file name}`



#### Dealing with imported data####

The imported data are stored in chunks of around 100 records as *import.csv* subfolders of *atom/tmp_results*. 
At the end of the process they will be all joined into *atom/tmp_results/import.csv*
This file is ready for import into AtoM. But you should verify the data before do so. 
Verification has two steps. 
* You need manually mark obsolete items (e.g. Looking for *Kolonie* my also give results as *Villenkolonie* or *Ferienkolonie*.  You can mark desired records with a dot in the `out` field. Non desired records should be marked with `x` in the  `out` field. Later the marks will be propagated to parent and/or child records. See *Reducing data* for more information.
* Check for duplicate items and respected hierarchy.  A special file *import_preparation.log* for this along with import.csv.
Note! It is usefull to not differ the names of different import.csv files. AtoM use them as part of premary key for look up tasks.
During the task the tools checks also which one of the wikidata keywords will fit the retrieved items. 


###Maintenance tasks###

**Important**: Most of the maintenance tasks are working direktly inside the mysql database. Please make sure you have a backup copy of youre database. Secondly you usually need to repopulate the search index after executing the task. This can be done by trying `sudo php /usr/share/nginx/atom/symfony search:populate` (if your installed AtoM on Linux and Nginx) .
You can execute more than one maintenance task before recreating the AtoM search index.

Some tasks depends on the predefined language (culture) and are executed only at records with the same ISO-639 code in their *culture* field. The culture is defined via the `CULTURE` constant in `atom/main/g.py` 

####Reducing data####
As already mentioned you always need to verify the created *atom/tmp_results/import.csv* manually. You can delete unwanted records or mark them with a `x` in `out` field. All non marked fields will be recompiled. Records marked with a dot will have priority. That means that if a child record is marked with a dot and a parent record with `x`, the parent record will not be deleted. 
You may also add or change data which has not been retrieved.
After verification try  `./atom-dm -m action=reduce-csv` to recreate the *atom/tmp_results/import.csv* file. The orginal file will be saved automatically.


#### Populate the keyword list using a Wikidata Query ####
The retrieved keywords are stored in *atom/data/keywords.json* . 
To use your own query replace parts of the query stored as WD_ENTITY_QUERY inside *atom/main/g.py* . Make sure that your query retrieves a column of Wikidata object URI called *?item*.

Try:

`./atom-dm -m action=index-wd-keywords`


#### Translate archival descriptions ####
**Important** : This task works directly inside the AtoM MySQL database! Make sure you have a copy for backup
The translation is made by <www.deepl.com>. You need an API key to proceed this task. 
Currently translation will pass from German to a target language which should be one of those proposed by the DeepL service.
You can change the configuration easily by modifying atom/helper/deepl.py

`./atom-dm -m action=translate-information-objects lang={ISO-639-1 code of target language}`


**Hint**: Make sure you have DeepL's cost control feature activated. Archival descriptions can contain lots of words.


Examples for the ISO-639 code are *en* or *fr*


#### Search & Replace words in archival description####

This task could be necessary if you get some systematic errors during the automatic translation, e.g. *Protected area* for *Schutzgebiet* instead of *Protectorate*
Currently replacements can only made inside the title, scope_and_content and archival_history fields of information_objects. The replacements will applied directly to the mysql database. 
Make sure you have a backup copy of youre database before you start with the task!

Try:

`./atom-dm -m action=replace`

You will then need to answer to some questions popping up:
  * Search term? - the term youre are searching for
  * Replace term? - the term you want to replace the searched term with
  * Culture ? - the ISO-639 code of the language where you want to replace terms. Of curse you need to use this language in your AtoM installation. 
  * Ignore case? - Should the search be case sensitive ? *Enter* for yes, any other input for no
  * Words only? - *Enter* if the search should include include entire words, anything else if the search term could als be part of a word.


#### Adding a creator to all child of a record####
For this task you need the slug of the already existing actor as well as the slug of the parent information object.
A slug is the last part of the path name inside the url (after the last "/")

Try: 
`./atom-dm -m action=add-creator oslug={slug of the parent information object} aslug={slug of the actor} `


#### Update all draft archival descriptions to published####
Try: 
`./atom-dm -m action=publish `


#### Finding more access points####
AtoM uses four types of access points from which places, subjects and genres where handled as taxonomy terms. On the other hand there are name access points which are stored as actors. Actors are also used to describe creators, repositories, right holders etc. Here we are just dealing with actors as name access points.

The basic approach of this task is to look inside information objects if certains keywords do match. In this case a relation between the information object and the actor (or the taxonomy term) will be created. The task will look only inside the fields *title* and *scope_and_content*. 

There a two different sources for keywords:
* The diffent access points which are already existing inside AtoM.  (`type=atom`)
* The *atom/data/keywords.json* file which is also used for data imports (`type=wd`)

In the first case there is a file *atom/data/access_points.json* which will be created first. This list is automatically updated if you start to look for new access points. But you can also modify this file manually, specially on adding the keys *indicators* or *exclusions* to a listed object. These key should be populated with lists where each list element can be noted as a regular expression, e.g. `"Bl[äa]tt[esrn]*"` will find *Blatt, Blatts, Blätter, Blattes* or *Blättern*. Cases will be ignored. 

It's indicated to change also the last_modified date of a modified keyword. Otherwise you will need to use all keywords iterating trough all information objects instead of iterating with the last modified keywords through the last added information objects. 

This task can easely take one day or more. Be warned!

Try:
`./atom-dm -m action=find-access-points-in-atom type={wd or atom} last-revision={yyyy-mm-dd hh:mm:ss or omitted } `

At the end of the task the tool will delete relation between access points and information objects if the same access point have already a relationship with an information object at a higher hierarchy level. 
It will also try to normalize the newly found access points. 

#### Normalizing access points####

This task is also executed automatically at the end of the *find-access-points-in-atom* task .

There different ways to get access points in disorder. Most often imported records from different institions are using different vocabularies. As one result it is not unusual to get multiple access point which are describing the same place, subject, genre or actor. The basis idea of this task ist to syncronize thoose entries with a more general vocabulary, which is the one created by Wikidata. For this each access point will get the URI of the correspondent Wikidata object. For actors the URI will be stored in the `description_identifier` field of the `actor` table. For all other access points the URI is stored as a source note with the predefined `CULTURE` constant. 

Due to the different nature of name access points at one hand and place, subject and genre access points on the other hand there are two different approaches:

For name access points try:

`./atom-dm -m action=normalize-name-access-points`

During this action several sub task will be executed:
* Unifying the Wikidata URI replacing http://wikidata.org/wiki/Q... by http://wikidata.org/entity/Q...
* Checking entity types (Person, Organisation, etc). Asking for right type if not given.
* Asking for the correspondent Wikidata URI if not present. This sub task can be typing `q`  
* Deduplicating actors with the same Wikidata URI. Deleting double object actor relations
* Reordering the name if actor is human to to *name, first name*
* Removing actors without object relations and Wikidata URI
* Completing labels in other languages using the Wikidata labels
* Filling out AtoM's actor description with data from Wikidata


For all other access points try:

`./atom-dm -m action=normalize-other-access-points`

During this action several sub task will be executed:
* Deduplicating object term relations
* Updating the *atom/data/access_points.json* file
* Writing Wikidata URI from the *atom/data/access_points.json* file into AtoM source note field
* Updating the access point label in AtoM if modified in the *atom/data/access_points.json* file
* Moving subject access points to place access points if there is already one with the same label
* Moving subject access points to name access points (actor) if there is already one with the same label
* Deduplicating access points whith same Wikidata URI
* Deduplicating access points whith same label


After the execution of this task nested sets have to be rebuild. 
Try:
`sudo php /usr/share/nginx/atom/symfony propel:build-nested-set` 

### Other aspects###

#### Geografical data index####
One part of the project site is a map browsing tool using OpenLayer and historical maps. Some of the maps come with their own name index. Those indexes where scanned and had to be inserted into a ElasticSearch index to become searchable.  
A special task has been created to populate the ElasticSearch index with data from `atom/data/atom_geo.csv`. The file is a tab seperated table without header and the following fields:
* id
* location label
* location label (ASCII)
* longitude
* latitude
* feature class
* country
* the map name for which the item has been indexed
* the "error", because due to the nature the printed index there are mostly just map coordinates who differs of a maximal error from the true location

To start to populate the index try:
`./atom-dm -m action=create-location-index`

**Note**: The task will delete the index before recreate the new index. 



#### Binary Classification of imported records using the torch ai framework####
It was tried to use a binary classification tool based on a neuronal network to determine if a imported record belongs to the general subject (here: German colonial history) or not. A bag of words where created.
Honestly spoken the try did not lead to the espected results. The error rate did not permit to use the results without verification. Specially the false negative error was rated to high. In consequence to much "good" records would be classified as "bad". The basic problem behind is that of incomplete information if using just archival descriptions instead of hypotetical full text information.
You will find the condensed efforts inside `atom/helper/ai.py` 

#### Binary Classification of imported records using a corpus based algorithm####
Better classification results are obtained when using an algorithm which uses the existing Wikidata corpus.

Main presumtions are:
* More search terms from the corpus will be found in the a text, more the text belongs to the subject of the corpus.
* The prediction will be better if "noisy" search terms will be excluded when analyzing the text.
* However "noisy" search terms can be used in combination with certain "attribute" terms. Example: *Schröder* is a very "noisy" search term which will produce lots of unwanted results. There a too many people with this name. Instead using *Schröder* with the attribut *Missionar* will more likely point to the the Missionar with name Schröder. Knowing from the Wikidata Description that *Schröder* is a *Missionar* he could also be found with attributes like *Bruder* or *Pater*. 
* Terms should be weighted depending on the classes there are instances from. Example: Terms describing German Colonies have more weigth than the terms describing  an instance of human.
* In presence of at least one "killing" term the entire text should be rated negative.
* A term has more weight if he is "rare". In the best case he is appearing only in the context of the corpus subject. Geting an idea of the weight of a term it is possible to do a simple search and counting the frequency over all results.
* If multiple terms from the corpus are found in the text and those terms are placed one near to another, more the text belongs to the subject of the corpus.

During the binary classification process of a specific record. All search terms from `atom/data/keywords.json`  will be weigthed and then checked against the record (title, scopen_and_content). 

For the moment most interfaces of the algorithm are defined inside the `predict_item` method of the `DataManager` class inside `atom/main/data_manager.py` . 

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
### Phyton Libraries used###

Among others:
  * slugify
  * lxml
  * logging
  * subprocess
  * torch (only if ai used)

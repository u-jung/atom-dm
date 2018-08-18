
  
### :red_circle: This is still work in progress. Please wait until the main project has been released. Thanks ###

# atom_data_manager #

Welcome,

This is a tool prepares data for the use inside AtoM. AtoM stands for Access to Memory. It is a web-based, open source application for standards-based archival description and access in a multilingual, multi-repository environment. (see also <https://www.accesstomemory.org>)


##Suported sites for data import##
###Archival descriptions###
DDB - www.deutsche-digitale-bibliothek.de / www.archivportal.de  (You need your API-Key to get access to their REST API
KAL - http://kalliope-verbund.info  (via SRU)

###authority data###
Wikidata - www.wikidata.org (You have to adopt the SPARQL-Queries inside this code)
Sigel-Verzeichnis  - http://sigel.staatsbibliothek-berlin.de


##Workflows##





### Here comes the Query to get a list of libraries and archives described by Wikidata ###
(You can use it at https://query.wikidata.org)

`SELECT distinct ?isil ?item ?itemLabel  WHERE {
  { ?item wdt:P31/wdt:P279* wd:Q166118. }
  UNION
  { ?item wdt:P31/wdt:P279* wd:Q7075. }
  optional {?item wdt:P791 ?isil}
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de,en, jp,it,es,fr,gr,ru,zh,dk,fi,nl" . } 
}
Order by ?isil`
# atom-dm

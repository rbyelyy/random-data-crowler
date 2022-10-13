# Scrap data from gov web sites#
* Grabbing data from some web sites using "urllib2" and "beautifulsoap" libraries. Pushing data into MS DB.
* MS db is updated only in case of occurring new one rows. (selecting per primary keys)
* Each site have separate table in DB

### How do I get set up? ###

* Configuration
 - pip install -r requirements.txt

### Dependencies ###
 - BeautifulSoup==3.2.1
 - beautifulscraper==1.0.3
 - beautifulsoup4==4.3.2
 - lxml==3.4.4
 - wsgiref==0.1.2
 - pypyodbc==1.3.3
 - eventlet==0.17.4
 - pandas==0.16.1
* Database configuration
 - florida
 - michigan
 - illinoise
 - main_gov

* Deployment instructions

### Contacts in case of problems? ###

* Owner / Admin - rbyelyy@gmail.com
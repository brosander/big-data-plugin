Pentaho Big Data Plugin
=======================

The Pentaho Big Data Plugin Project provides support for an ever-expanding Big Data community within the Pentaho ecosystem. It is a plugin for the Pentaho Kettle engine which can be used within Pentaho Data Integration (Kettle), Pentaho Reporting, and the Pentaho BI Platform.

Use
---
This plugin provides big data oriented steps and job entries for Pentaho Kettle.  These can be used to interoperate with various [big data technologies]:(https://help.pentaho.com/Documentation/6.0/0L0/040/040).  It also provides [Hive](https://help.pentaho.com/Documentation/6.0/0D0/160/010#Hive2) and [Impala](https://help.pentaho.com/Documentation/6.0/0D0/160/010#Impala) support across the Pentaho Stack as well as VFS providers for big data sources.

Development
--------
The Pentaho Big Data Plugin is built with Apache Maven. All you'll need to get started is Maven 3.2.2 or newer to build the project.

    $ git clone git://github.com/pentaho/big-data-plugin.git
    $ cd big-data-plugin
    $ mvn clean install

The Big Data Plugin is currently being moved into OSGi.  This means that there is a Legacy plugin that can be extracted into Kettle's plugin directy.  Its archive will be located at legacy/target.

The other subcomponents have been installed to your local Maven repository by the build process.  This means that Karaf within Kettle will use them if it doesn't find them in the system repo inside PDI.

Further Reading
---------------
Additional documentation is available on the Community wiki: [Big Data Plugin for Java Developers](http://wiki.pentaho.com/display/BAD/Getting+Started+for+Java+Developers)

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.

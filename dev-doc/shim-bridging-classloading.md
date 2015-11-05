As a step towards a more flexible architecture less constrained by the shims, we have been refactoring the steps and job entries to use higher level services.  They follow the following pattern:

api
---
A higher level api that exposes big data capabilities as services with locators that take a NamedCluster as their argument.

These should only rely on kettle-core, the metastore, and other api bundles.

They will typically be made up of [a service interface](https://github.com/pentaho/big-data-plugin/blob/master/api/pig/src/main/java/org/pentaho/bigdata/api/pig/PigService.java), [a service locator](https://github.com/pentaho/big-data-plugin/blob/master/api/pig/src/main/java/org/pentaho/bigdata/api/pig/PigServiceLocator.java), and a [factory](https://github.com/pentaho/big-data-plugin/blob/master/api/pig/src/main/java/org/pentaho/bigdata/api/pig/PigServiceFactory.java).

impl/shim 
---------
An initial implementation of the api that delegates to the shim.

kettle-plugins
--------------
The step and job entry logic and dialog code.

# Publishing Conda libraries

This repository template is set up to publish a Conda library into Foundry. The ``build.gradle`` file configures the publish task to only run when the repository is tagged. You can create a new tag from the "Branches" tab.

By default, the repository's name at creation time is used as the name for the Conda package. It is possible to change the name of the package by updating the ``condaPackageName`` variable in the ``gradle.properties`` file. Note that since this is a hidden file, you will need to enable "Show hidden files and folders".

*Important:* underscores in the repository name are rewritten to dash. For example, if your repository is named `my_library`, then the library will be published as `my-library`.

## Consuming Conda Libraries

Consumers will require read access on this repository to be able to consume the libraries it publishes. They can search for them in the *Libraries* section on the left-hand side in the consuming code repository. This will automatically add the dependency to ``meta.yaml`` and configure the appropriate Artifacts backing repositories.

Adding a library to your project will install packages from the source directory. The source directory defaults to ``src/`` and we recommend not changing this. You still need to import packages before you can use them in your module. Be aware that you have to import package name and not library name (in this template, the package name is ``myproject``).

### Example

Let's say your library structure is:

```
conda_recipe/
src/
  examplepkg/
    __init__.py
    mymodule.py
  otherpkg/
    __init__.py
    utils.py
  setup.cfg
  setup.py
```

And in ``gradle.properties``, the value of ``condaPackageName`` is ``mylibrary``.

When consuming this library, the consuming repository's ``conda_recipe/meta.yaml`` file will contain:

```
requirements:
  run:
    - mylibrary
```

Then the packages, which in this example are ``examplepkg`` and ``otherpkg``, can be imported as follows:

```
import examplepkg as E
from examplepkg import mymodule
from otherpkg.utils import some_function_in_utils
```

Note that the import will fail if the package does not include a file named ``__init__.py``

## Testing

Unit tests can be evaluated automatically as part of CI checks. You can enable this by uncommenting the following line in `build.gradle`:

```
// Apply the testing plugin
apply plugin: 'com.palantir.transforms.lang.pytest-defaults'
```

You can find an example unit test inside the `src/test` directory. Please refer to the [documentation](https://www.palantir.com/docs/foundry/transforms-python/unit-tests/#enabling-tests) for more information.

# CCDA_OMOP_by_Python

This is a project to convert CCDA documents to OMOP CDM format in Python.
The bulk of the work is in the prototype_2 directory. Further instruction is in a README.md file there.


## BADGE
[![Prototype 2](https://github.com/cladteam/CCDA_OMOP_by_Python/actions/workflows/prototype_2.yml/badge.svg)](https://github.com/cladteam/CCDA_OMOP_by_Python/actions/workflows/prototype_2.yml)

## Sub Projects/Directories
This repositor has siblings that might be of interest.
- CCDA-tools is a collection of different file snoopers, scripts that find and show a XML elements with a certain tag, like id, code or section.
 https://github.com/chrisroederucdenver/CCDA-tools
- CCDA-data
sample data is in it's own repo. The directory has been flattened compared to earlier work.
https://github.com/chrisroederucdenver/CCDA-data
- CCDA_Private is a private repository for keeping and sharing the vocabularies used here. After dealing with licenses this data, the OMOP concepts can be dowloaded from athena.ohdsi.org.
https://github.com/chrisroederucdenver/CCDA_OMOP_Private


Code here, prototype_2/value_transformations.py  loads the concept map from a file map_to_standard.csv from the local top leve.
It is not included in this repo however. It is in CCDA-tools.  Running locally, for the moment, requires the file to be copied over (or linked).
(now owned by cladteam)



# Vehicle/software import script

## Prerequisites
The git, php8, and php-composer must all be installed.

## Preparation steps

* Note: this is all done on the command line. *
  
1. Clone this repository
   It doesn't matter where you clone this to; just make sure your user has read/write permissions in the directory.
   ```console
   git clone https://github.com/cweersma/vehicle_import.git
   ```
2. Install Velox Server
   A composer.json file is included. You will only need to run the `composer install` command to install the Velox Server library.
   ```console
   cd vehicle_import
   composer install
   ```
   

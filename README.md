# Cyndx DE Challenge Solution

The proposed solution reads the input dataset line by line, parses each
line, and conforms the phone to the format `+country_code (area_code) 
number[1:3]-number[4:7]`. For the persistence purposes (in a case
of the low availability of the remote database), the script saves
each modified line on the local disk.

The second stage connects to the remote PostgreSQL server (arguments
are loaded from a local config file for the security reasons), bulk loads
the modified dataset from the local disk, extracts domain from the
web addresses using regex, and performs a join with `companies` table
to save the results in the remote database.
The domain extraction is necessary to speed up the join query, which,
otherwise, would need to run full string match (extremely slow).

Future improvements to the code could include a more intelligent
algorithm in parsing phone numbers by using checks against existing
country and area codes and matching them with the domain names or
other relevant auxiliary data (office location, business 
registration, etc.).
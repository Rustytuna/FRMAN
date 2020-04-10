# Adjuftments

Adjuftments is an expense application created by Justin Flannery (`@juftin`). The primary function of this application is the running of a script at `Adjuftments/adjuftments.py`.

### Important Notes:

-   The credentials file can be decrpyted by calling the following script and passing the proper credentials/Arguments (`Adjuftments/encryption.py`).
-   Before the application can be run a "Clean Start" must be run to initialize the database and ingest all necessary data. You can kick off this process by calling the `clean` function (`Adjuftments/functions.py clean`).

### Docker Notes:

-   This application can be run directly from a docker container (`python:3.7.3`)
-   All dependencies are included in `requirements.txt`
-   The docker container can be built and initialized via the `rundocker` bash file.
-   Passing decryption credentials during the build is required
-   A simple restart command to run is `docker stop adjuftments ; docker rm adjuftments ; docker rmi juftin/adjuftments:latest ; ./rundocker`

<br/>
<br/>

###### Adjuftments was built with ❤️ in Denver, CO

<br/>
<br/>

# TODO:

-   Code Cleanup
    -   fix variable names
-   Flask Webserver
    -   Create an HTML + CSS files
    -   Dashboard Dataframe -> HTML
    -   Traefik Hosting for HTML

# Qualtrics Post Appointment Survey Response Visualization Script.
Welcome! You can find the background script for the Qualtrics Post Appointment Survey Dashboard here. 

## Strcture of the program:
There are two main parts to this application: one part that displays and visualizes the data; the other part that fetches and processes the data. The code in this repository is responsible for the former, while the former code can be found [here](https://github.com/kshuta/qualtrics_vis). 

This code is hosted in Heroku, however there aren't instances you must access the endpoint directly. The display/visualization code accesses the endpoint automatically, fetching the data from Qualtrics and recording them to a shared Postgres database.

The latter part uses Python to fetch information from the Qualtrics API and processes the data, finally saving them into a Postgres database hosted in Heroku.
Most of the code for fetching information can be found on the [Qualtrics API documentation](https://api.qualtrics.com/).
The data processing part is a bit complicated, but you can probably read through the code. Everything is within `process_data.py`.

# Simulating Wind Energy

## Intro
I was recently having a conversation with a family member about these massive wind farms that we see when traveling to visit them. We both speculated that there had to be some method to the madness around how they are placed. The directions they faced seemed completely random and most faced at slightly different angles. Theories included obvious factors like historical weather patterns and available land size. I thought this would be a great way to dive into my fascination with simulations.

## Setup
This repo will create a weather table that consists of about four years of historical weather readings. It will then create a Delta Live Tables pipeline that will start automatically and start running simulations of a windfarm setup.

After the pipeline starts a streaming visulization will begin that will track data flowing through the different DLT steps. Once the `valid_mill_placement` step has records in it then the next cell can be ran to show placement of the mills on a map

## Pre-req
Fill out the DatabaseName, DatabricksHost and PAT in the widget bar at the top

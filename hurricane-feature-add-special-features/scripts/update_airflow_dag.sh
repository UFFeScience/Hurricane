#!/bin/bash

echo "# REMOVENDO DAGS ..."
sudo rm -rf ~/airflow/dags/hurricane/*

echo "# COPIANDO DAG ..."
sudo cp -R /home/maiconbanni/Documentos/UFF/hurricane/dags/* ~/airflow/dags/

echo "# FIM!"
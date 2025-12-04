# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = "data_zone"
        self.base_dir_checkpoint ="checkpoint"
        self.db_name = "sbit_db"
        self.maxFilesPerTrigger = 1000

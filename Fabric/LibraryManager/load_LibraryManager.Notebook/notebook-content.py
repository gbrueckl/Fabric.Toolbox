# Fabric notebook source


# CELL ********************

library_path = "abfss://ca0a79b9-9c03-40b5-9d6a-cfd3fda1c31e@onelake.dfs.fabric.microsoft.com/2925655f-0293-4f32-8bc6-86ab989099a7/Files/Libraries/LibraryManager.zip"
print(f"Loading LibraryManager from '{library_path}' ... ", end = "")
sc.addPyFile(library_path)
print("Done!")

from LibraryManager import *

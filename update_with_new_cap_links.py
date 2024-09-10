## ---------------------------------------------------------------------------##
# Beatrix Haddock
# Aug 28, 2024
# Script to update url and folder links using the sharepoint link to the folder
# containing the CAP.
## ---------------------------------------------------------------------------##
import pandas as pd

def main():
    cap_links = pd.read_csv("cap_sharepoint_links.txt", sep="\t")
    cap_links['url'] = cap_links.cap_folder_sharepoint_path.str.replace(":f:/r/","").str.split("/Shared%20Documents", expand=True)[0]
    cap_links['folder'] = cap_links.cap_folder_sharepoint_path.str.partition(":f:/r", expand=True)[2].str.partition("?", expand=True)[0]

    print("This script assumes that the 'cap_folder_sharepoint_path' column is up to date in 'cap_sharepoint_links.txt', and correspondingly updates the 'url' and 'folder' columns!")

    cap_links.to_csv("cap_sharepoint_links.txt", sep="\t", index=False)

if __name__=="__main__":
    main()

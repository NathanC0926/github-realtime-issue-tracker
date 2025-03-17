# GitHub Realtime Issue Tracker

## 🚀 Setup Instructions
***Prerequisites: Docker***

1. **Run the Setup Using Make. For the first-time setup, run:**
    ```bash
    make up
    ```
    ***Look at the Makefile for more commands.***
    
2. **📥 Downloading the Dataset**
    ```bash
    cd data
    wget https://data.gharchive.org/2024-12-{01..31}-{0..23}.json.gz
    ```
    If your computer is trash, download fewer days by adjusting the {0..31} range.

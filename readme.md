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
    for month in {01..03}; do
        for day in {01..31}; do
            for hour in {0..23}; do
                curl -O https://data.gharchive.org/2025-${month}-${day}-${hour}.json.gz
            done
        done
    done
    ```
    If your computer is trash, download fewer months/days/hours by adjusting the {0..n} range.

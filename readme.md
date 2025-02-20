# GitHub Realtime Issue Tracker

## 🚀 Setup Instructions

1. **Clone the repository and navigate into the project directory**  
   ```bash
   git clone https://github.com/YOUR-USERNAME/github-realtime-issue-tracker.git
   cd github-realtime-issue-tracker
    ```
2. **Run the setup script to initialize the environment**
```bash
./setup.sh
```
***This will:***
- Set up a virtual environment
- Install required dependencies (pyspark, sql-magic)
- Create an empty data/ folder if it doesn't exist
3. **📥 Downloading the Dataset**
```bash
cd data
wget https://data.gharchive.org/2025-01-01-{0..23}.json.gz
```
If your computer is trash, download fewer hours by adjusting the {0..23} range.
# 🗄️ Universal Database Manager

A powerful, lightweight web-based database management tool built with **Flask**. It provides a unified interface to manage both **MongoDB** (NoSQL) and **PostgreSQL** (SQL) databases seamlessly.

![Database Manager Preview](https://img.shields.io/badge/Status-Live-success)
![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)
![Flask](https://img.shields.io/badge/Flask-2.x-lightgrey?logo=flask)
![MongoDB](https://img.shields.io/badge/MongoDB-Supported-green?logo=mongodb)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supported-blue?logo=postgresql)

## 🚀 Live Demo
Check out the deployed version here: [Database Manager on Azure](https://flaskapplication-dwfea2dshca6h4et.eastasia-01.azurewebsites.net/)

---

## ✨ Features

- **🔌 Universal Connection Builder**: Easily generate connection strings for MongoDB and PostgreSQL.
- **📁 Multi-Database Support**: Switch between different databases on the fly.
- **🛠️ Schema Management**: 
    - Create/Drop Databases.
    - Create/Drop Tables (PostgreSQL) and Collections (MongoDB).
    - Interactive SQL Schema builder for PostgreSQL.
- **📝 Full CRUD Operations**: 
    - View records in a modern, responsive table.
    - Add, Edit, and Delete records via dynamic modals.
    - Support for JSONB columns in PostgreSQL.
- **🎨 Premium UI**: A clean, modern interface with sticky headers/columns and glassmorphism effects.

---

## 🛠️ Tech Stack

- **Backend**: Python, Flask
- **Drivers**: `pymongo` (MongoDB), `psycopg2` (PostgreSQL)
- **Frontend**: Vanilla JS, Modern CSS (Embedded templates)
- **Deployment**: Azure Web Apps

---

## 💻 Local Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Hari-Prasath-M91/Database-Manager.git
   cd Database-Manager
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Application**:
   ```bash
   python app.py
   ```
   The application will be available at `http://localhost:8000`.

---

## ☁️ Deployment to Azure

You can publish this application (or any Flask/Django/FastAPI app) to Azure using a single command in PowerShell.

### 1. Create Resources
- Create a new Azure Web App to host your application.
- Set up a PostgreSQL database on Azure for your testing environment.

### 2. Install Azure CLI
If you haven't installed it yet, run:
```powershell
winget install --exact --id Microsoft.AzureCLI
```

### 3. Login
```powershell
az login
```

### 4. Setup Azure for Deployment
Configure Azure to build the application (install requirements) during the deployment process:
```powershell
az webapp config appsettings set `
--name <your-app-name> `
--resource-group <your-resource-group> `
--settings SCM_DO_BUILD_DURING_DEPLOYMENT=true
```

### 5. Push the Code
Zip your project files and push them to Azure. **Ensure your `app.py` is set to run on port 8000.**
```powershell
az webapp deploy `
--name <your-app-name> `
--resource-group <your-resource-group> `
--src-path .\app.zip
```

---

## 📄 License
This project is open-source and available under the MIT License.

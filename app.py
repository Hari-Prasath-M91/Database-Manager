from flask import Flask, render_template_string, request, jsonify
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from bson.objectid import ObjectId

app = Flask(__name__)

class DatabaseManager:
    def __init__(self, db_type, connection_string):
        self.db_type = db_type
        self.connection_string = connection_string.rstrip('/')
        self.connection = None
        
    def _get_postgres_conn_string(self, target_db='postgres'):
        if self.connection_string.count('/') > 2:
            base = self.connection_string.rsplit('/', 1)[0]
        else:
            base = self.connection_string
        return f"{base}/{target_db}"

    def connect(self):
        if self.db_type == 'mongodb':
            client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            client.server_info()
            if self.connection_string.count('/') > 2:
                parts = self.connection_string.split('?')[0].split('/')
                if parts[-1]:
                    self.connection = client[parts[-1]]
                else:
                    self.connection = client
            else:
                self.connection = client
        else:
            if self.connection_string.count('/') <= 2:
                conn_str = f"{self.connection_string}/postgres"
            else:
                conn_str = self.connection_string
            self.connection = psycopg2.connect(conn_str)

    def get_databases(self):
        if self.db_type == 'mongodb':
            client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            return client.list_database_names()
        else:
            conn_str = self._get_postgres_conn_string('postgres')
            conn = psycopg2.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
            dbs = [row[0] for row in cursor.fetchall()]
            conn.close()
            return dbs
    
    def get_collections_or_tables(self):
        if self.db_type == 'mongodb':
            if isinstance(self.connection, MongoClient): return [] 
            return self.connection.list_collection_names()
        else:
            cursor = self.connection.cursor()
            cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            return [row[0] for row in cursor.fetchall()]

    def get_table_columns(self, table_name):
        if self.db_type == 'mongodb': return []
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (table_name,))
        return [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]

    def _get_pk_column(self, table_name):
        if self.db_type == 'mongodb': return '_id'
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name
              AND kcu.table_schema = tco.table_schema
            WHERE tco.constraint_type = 'PRIMARY KEY'
              AND kcu.table_name = %s
        """, (table_name,))
        row = cursor.fetchone()
        return row[0] if row else None 

    def get_all_records(self, collection, limit=100):
        if self.db_type == 'mongodb':
            records = list(self.connection[collection].find().limit(limit))
            for r in records: r['_id'] = str(r['_id'])
            return records
        else:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Select hidden ctid to handle tables without PKs
            cursor.execute(f'SELECT *, ctid::text AS _pg_ctid FROM "{collection}" LIMIT %s', (limit,))
            rows = [dict(row) for row in cursor.fetchall()]
            
            pk = self._get_pk_column(collection)
            cleaned_rows = []
            for row in rows:
                # Determine 'real_id' for editing logic
                real_id = row.get(pk) if pk else row.get('_pg_ctid')
                
                # Assign to 'id' property so the UI buttons work uniformly
                if 'id' not in row:
                    row['id'] = real_id

                # Unpack JSONB 'data' column if it's a document store table
                if 'data' in row and len(row) <= 4: 
                    flat = row['data'] if isinstance(row['data'], dict) else {'value': row['data']}
                    flat['id'] = real_id
                    if '_pg_ctid' in row: flat['_pg_ctid'] = row['_pg_ctid']
                    if 'created_at' in row: flat['created_at'] = str(row['created_at'])
                    cleaned_rows.append(flat)
                else:
                    cleaned_rows.append(row)
            return cleaned_rows

    def create_record(self, collection, data):
        if self.db_type == 'mongodb':
            res = self.connection[collection].insert_one(data)
            return str(res.inserted_id)
        else:
            cursor = self.connection.cursor()
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (collection,))
            table_columns = {row[0] for row in cursor.fetchall()}
            
            pk = self._get_pk_column(collection)
            valid_data = {k: v for k, v in data.items() if k in table_columns}

            if valid_data:
                cols = ', '.join(f'"{k}"' for k in valid_data.keys())
                vals = ', '.join(['%s'] * len(valid_data))
                
                if pk:
                    q = f'INSERT INTO "{collection}" ({cols}) VALUES ({vals}) RETURNING "{pk}"'
                    cursor.execute(q, list(valid_data.values()))
                    ret = cursor.fetchone()
                    self.connection.commit()
                    return ret[0]
                else:
                    # No PK? Just insert.
                    q = f'INSERT INTO "{collection}" ({cols}) VALUES ({vals})'
                    cursor.execute(q, list(valid_data.values()))
                    self.connection.commit()
                    return None
                    
            elif 'data' in table_columns:
                q = f'INSERT INTO "{collection}" (data) VALUES (%s) RETURNING id'
                cursor.execute(q, (json.dumps(data),))
                self.connection.commit()
                return cursor.fetchone()[0]
            else:
                raise Exception(f"Schema Mismatch: keys {list(data.keys())} do not match table columns")

    def update_record(self, collection, record_id, data):
        if self.db_type == 'mongodb':
            # Ensure we don't try to overwrite the immutable _id field
            if '_id' in data:
                del data['_id']
            
            # Use replace_one instead of update_one($set)
            # This ensures keys deleted in the UI are deleted in the DB
            res = self.connection[collection].replace_one(
                {'_id': ObjectId(record_id)}, 
                data
            )
            
            # Use matched_count because if data hasn't changed, modified_count will be 0
            # but the operation was still successful.
            return res.matched_count > 0
        else:
            cursor = self.connection.cursor()
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (collection,))
            table_columns = {row[0] for row in cursor.fetchall()}
            
            pk = self._get_pk_column(collection)
            valid_data = {k: v for k, v in data.items() if k in table_columns}

            if valid_data:
                sets = ', '.join(f'"{k}" = %s' for k in valid_data.keys())
                if pk:
                    q = f'UPDATE "{collection}" SET {sets} WHERE "{pk}" = %s'
                else:
                    q = f'UPDATE "{collection}" SET {sets} WHERE ctid = %s::tid'
                    
                cursor.execute(q, list(valid_data.values()) + [record_id])
            elif 'data' in table_columns:
                q = f'UPDATE "{collection}" SET data = %s WHERE id = %s'
                cursor.execute(q, (json.dumps(data), record_id))
            else:
                 raise Exception("Schema Mismatch")

            self.connection.commit()
            return cursor.rowcount > 0

    def delete_record(self, collection, record_id):
        if self.db_type == 'mongodb':
            res = self.connection[collection].delete_one({'_id': ObjectId(record_id)})
            return res.deleted_count > 0
        else:
            cursor = self.connection.cursor()
            pk = self._get_pk_column(collection)
            
            if pk:
                q = f'DELETE FROM "{collection}" WHERE "{pk}" = %s'
            else:
                q = f'DELETE FROM "{collection}" WHERE ctid = %s::tid'
                
            cursor.execute(q, (record_id,))
            self.connection.commit()
            return cursor.rowcount > 0
            
    def create_database(self, db_name):
        if self.db_type == 'mongodb':
            MongoClient(self.connection_string)[db_name]['_init'].insert_one({'x':1})
            MongoClient(self.connection_string)[db_name]['_init'].drop()
        else:
            conn_str = self._get_postgres_conn_string('postgres')
            conn = psycopg2.connect(conn_str)
            conn.autocommit = True
            conn.cursor().execute(f'CREATE DATABASE "{db_name}"')
            conn.close()

    def drop_database(self, db_name):
        if self.db_type == 'mongodb':
            MongoClient(self.connection_string).drop_database(db_name)
        else:
            conn_str = self._get_postgres_conn_string('postgres')
            conn = psycopg2.connect(conn_str)
            conn.autocommit = True
            conn.cursor().execute(f'DROP DATABASE "{db_name}"')
            conn.close()

    def create_collection(self, name, schema=None):
        if self.db_type == 'mongodb':
            self.connection.create_collection(name)
        else:
            cursor = self.connection.cursor()
            if schema and schema.strip():
                cursor.execute(f'CREATE TABLE "{name}" ({schema})')
            else:
                cursor.execute(f'CREATE TABLE "{name}" (id SERIAL PRIMARY KEY, data JSONB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            self.connection.commit()

    def drop_collection(self, name):
        if self.db_type == 'mongodb':
            self.connection[name].drop()
        else:
            cursor = self.connection.cursor()
            cursor.execute(f'DROP TABLE "{name}"')
            self.connection.commit()

# --- HTML TEMPLATE ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Database Manager</title>
    <style>
        body { font-family: -apple-system, system-ui, sans-serif; padding: 20px; background: #f4f6f8; color: #333; }
        .container { max-width: 1400px; margin: 0 auto; background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); display: flex; flex-direction: column; height: fit-content; }
        .header { margin-bottom: 20px; padding-bottom: 20px; border-bottom: 2px solid #f0f0f0; }
        h2 { margin-top: 0; color: #2c3e50; }
        .builder-box { background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #e9ecef; margin-bottom: 15px; }
        .builder-row { display: flex; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
        .builder-field { flex: 1; min-width: 120px; }
        .builder-field label { display: block; font-size: 11px; font-weight: bold; color: #6c757d; margin-bottom: 4px; text-transform: uppercase; }
        .builder-field input, .builder-field select { width: 100%; padding: 8px; border: 1px solid #ced4da; border-radius: 4px; font-size: 13px; box-sizing: border-box;}
        .main-connection-row { display: flex; gap: 10px; align-items: flex-end; }
        .conn-input-group { flex-grow: 1; }
        .conn-input-group input { width: 100%; padding: 10px; font-family: monospace; border: 1px solid #ced4da; border-radius: 4px; background: #fff; color: #2c3e50; }
        button { cursor: pointer; padding: 10px 16px; border: none; border-radius: 4px; font-weight: 600; transition: opacity 0.2s; }
        button:hover { opacity: 0.9; }
        .btn-primary { background: #007bff; color: white; }
        .btn-success { background: #28a745; color: white; }
        .btn-danger { background: #dc3545; color: white; }
        .btn-warning { background: #ffc107; color: #212529; }
        .status { padding: 10px; border-radius: 4px; margin: 10px 0; display: none; font-weight: 500; }
        .status.error { background: #fee2e2; color: #991b1b; border: 1px solid #fecaca; }
        .status.success { background: #dcfce7; color: #166534; border: 1px solid #bbf7d0; }
        .db-chips { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
        .chip { padding: 6px 12px; background: #e9ecef; border-radius: 20px; cursor: pointer; font-size: 13px; font-weight: 500; transition: all 0.2s; }
        .chip:hover { background: #dee2e6; }
        .chip.active { background: #007bff; color: white; box-shadow: 0 2px 4px rgba(0,123,255,0.3); }
        .grid { display: grid; grid-template-columns: 260px 1fr; gap: 25px; margin-top: 20px; flex: 1; min-height: 0; }
        .sidebar { background: #f8f9fa; padding: 20px; border-radius: 8px; border: 1px solid #e9ecef; display: flex; flex-direction: column; }
        .collection-list { flex: 1; overflow-y: auto; }
        .collection-item { padding: 8px 12px; margin-bottom: 4px; cursor: pointer; border-radius: 4px; font-size: 14px; }
        .collection-item:hover { background: #e9ecef; }
        .collection-item.active { background: #e7f1ff; color: #007bff; font-weight: 600; }
        .content { display: flex; flex-direction: column; min-width: 0; }
        
        /* --- SCROLL & STICKY TABLE CSS --- */
        .table-scroll-container {
            flex: 1;
            overflow: auto; 
            border: 1px solid #eee;
            border-radius: 8px;
            background: white;
            position: relative;
        }
        
        table { 
            width: 100%; 
            border-collapse: separate; 
            border-spacing: 0;
            margin-top: 0; 
            font-size: 13px;
            white-space: nowrap; 
        }
        
        th, td { 
            padding: 10px 15px; 
            border-bottom: 1px solid #eee; 
            border-right: 1px solid #f9f9f9;
            text-align: left; 
        }
        
        /* 1. Sticky Header */
        th { 
            position: sticky;
            top: 0;
            z-index: 10;
            background: #f1f3f5;
            color: #495057; 
            font-weight: 600; 
            border-bottom: 2px solid #ddd;
        }
        
        /* 2. Sticky Actions Column (Right Side) */
        th:last-child, td:last-child {
            position: sticky;
            right: 0;
            background: white; /* Opaque background to hide scroll */
            border-left: 2px solid #f0f0f0;
            box-shadow: -2px 0 5px rgba(0,0,0,0.02);
            z-index: 5;
        }
        
        /* 3. The Corner Piece (Top Right Header) needs highest index */
        th:last-child { 
            z-index: 20; 
            background: #f1f3f5;
        }
        
        tr:hover td { background: #f8f9fa; }
        tr:hover td:last-child { background: #f8f9fa; }

        /* MODALS */
        .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; }
        .modal-content { background: white; width: 90%; max-width: 600px; margin: 50px auto; padding: 25px; border-radius: 8px; box-shadow: 0 10px 25px rgba(0,0,0,0.1); max-height: 90vh; overflow-y: auto; }
        .modal h3 { margin-top: 0; border-bottom: 1px solid #eee; padding-bottom: 10px; }
        .modal-footer { margin-top: 20px; display: flex; justify-content: flex-end; gap: 10px; border-top: 1px solid #eee; padding-top: 15px; }
        .dynamic-field { margin-bottom: 15px; }
        .dynamic-field label { display: block; font-weight: bold; font-size: 12px; margin-bottom: 5px; color: #555; }
        .dynamic-field input { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
        
        /* SCHEMA BUILDER STYLES */
        .schema-row { display: grid; grid-template-columns: 2fr 1.5fr 0.5fr 0.5fr 0.5fr 30px; gap: 8px; align-items: center; margin-bottom: 8px; background: #f9f9f9; padding: 5px; border-radius: 4px; }
        .schema-row input, .schema-row select { padding: 6px; font-size: 13px; border: 1px solid #ddd; border-radius: 3px; width: 100%; }
        .schema-header { display: grid; grid-template-columns: 2fr 1.5fr 0.5fr 0.5fr 0.5fr 30px; gap: 8px; margin-bottom: 5px; font-size: 11px; font-weight: bold; color: #666; padding-left: 5px; }
        .schema-row input[type="checkbox"] { width: auto; }
        .pg-only { display: none; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>🗄️ Universal Database Manager</h2>
            <div class="builder-box">
                <div style="font-size: 12px; font-weight: bold; margin-bottom: 10px; color: #007bff;">🔌 CONNECTION BUILDER</div>
                <div class="builder-row">
                    <div class="builder-field" style="flex: 0 0 120px;">
                        <label>DB Type</label>
                        <select id="b-type" onchange="updateBuilderDefaults()">
                            <option value="mongodb">MongoDB</option>
                            <option value="postgresql">PostgreSQL</option>
                        </select>
                    </div>
                    <div class="builder-field" style="flex: 2;">
                        <label>Host</label>
                        <input id="b-host" placeholder="localhost" value="localhost" oninput="buildString()">
                    </div>
                    <div class="builder-field" style="flex: 0 0 80px;">
                        <label>Port</label>
                        <input id="b-port" placeholder="27017" value="27017" oninput="buildString()">
                    </div>
                </div>
                <div class="builder-row">
                    <div class="builder-field">
                        <label>Username (Optional)</label>
                        <input id="b-user" placeholder="root" oninput="buildString()">
                    </div>
                    <div class="builder-field">
                        <label>Password (Optional)</label>
                        <input id="b-pass" type="password" placeholder="secret" oninput="buildString()">
                    </div>
                </div>
            </div>
            <div class="main-connection-row">
                <div class="conn-input-group">
                    <label style="display:block; font-size: 12px; color: #666; margin-bottom: 5px;">Generated Connection String</label>
                    <input type="text" id="conn-string" placeholder="mongodb://localhost:27017">
                </div>
                <button class="btn-primary" onclick="connectToDatabase()">Connect &rarr;</button>
            </div>
            <div id="status" class="status"></div>
            <div id="db-section" style="display:none; margin-top: 20px; border-top: 1px solid #eee; padding-top: 15px;">
                <label style="font-weight: bold; color: #333;">Available Databases:</label>
                <div id="db-list" class="db-chips"></div>
                <div style="margin-top: 15px;">
                    <button class="btn-success" style="font-size: 12px;" onclick="openModal('db')">+ Create New DB</button>
                    <button class="btn-danger" style="font-size: 12px;" onclick="dropDatabase()">Delete Selected DB</button>
                </div>
            </div>
        </div>
        <div class="grid" id="main-interface" style="opacity: 0.5; pointer-events: none;">
            <div class="sidebar">
                <h4 style="margin-top: 0;">Tables / Collections</h4>
                <div class="collection-list" id="col-list"></div>
                <div style="margin-top: 15px; border-top: 1px solid #eee; padding-top: 10px;">
                    <button class="btn-success" style="width: 100%; margin-bottom: 5px;" onclick="openModal('col')">New Table/Collection</button>
                    <button class="btn-danger" style="width: 100%;" onclick="dropCollection()">Delete Selected</button>
                </div>
            </div>
            <div class="content">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h3 style="margin: 0;">Data View</h3>
                    <div style="display: flex; gap: 10px;">
                        <button class="btn-primary" onclick="loadRecords()">↻ Refresh</button>
                        <button class="btn-success" onclick="openRecordModal()">+ Add Record</button>
                    </div>
                </div>
                <div id="data-view" class="table-scroll-container">
                    <div style="text-align: center; color: #999; padding-top: 80px;">Select a collection to view records</div>
                </div>
            </div>
        </div>
    </div>
    
    <div id="modal-db" class="modal">
        <div class="modal-content">
            <h3>Create New Database</h3>
            <input id="new-db-name" placeholder="Database Name" class="dynamic-field input" style="width:100%">
            <div class="modal-footer">
                <button onclick="closeModals()">Cancel</button>
                <button class="btn-success" onclick="createDatabase()">Create</button>
            </div>
        </div>
    </div>

    <div id="modal-col" class="modal">
        <div class="modal-content" style="max-width: 800px;">
            <h3>New <span id="new-type-label">Collection</span></h3>
            <div class="dynamic-field">
                <label>Name</label>
                <input id="new-col-name" placeholder="e.g. users, products" style="width:100%; font-size:16px;">
            </div>
            <div id="pg-schema-builder" class="pg-only" style="margin-top: 20px; border-top: 1px solid #eee; padding-top: 10px;">
                <label style="font-weight:bold; color:#007bff; margin-bottom:10px; display:block;">Define Schema Columns</label>
                <div class="schema-header">
                    <div>Column Name</div>
                    <div>Data Type</div>
                    <div title="Primary Key">PK</div>
                    <div title="Not Null">NN</div>
                    <div title="Unique">UN</div>
                    <div></div>
                </div>
                <div id="schema-rows"></div>
                <button class="btn-primary" style="font-size: 12px; margin-top: 10px;" onclick="addSchemaRow()">+ Add Column</button>
                <div style="margin-top: 20px;">
                    <label style="font-size: 11px; color: #888;">Preview SQL (Read Only):</label>
                    <textarea id="sql-preview" readonly style="width: 100%; height: 60px; font-family: monospace; font-size: 11px; color: #555; background: #f0f0f0; border: 1px solid #ddd;"></textarea>
                </div>
            </div>
            <div class="modal-footer">
                <button onclick="closeModals()">Cancel</button>
                <button class="btn-success" onclick="createCollection()">Create</button>
            </div>
        </div>
    </div>
    
    <div id="modal-record" class="modal">
        <div class="modal-content">
            <h3 id="record-modal-title">Edit Record</h3>
            <div id="form-container"></div>
            <div id="add-field-btn" style="display:none; margin-top:10px;">
                <button class="btn-primary" style="font-size: 12px; padding: 5px 10px;" onclick="addKvField()">+ Add Field</button>
            </div>
            <div class="modal-footer">
                <button onclick="closeModals()">Cancel</button>
                <button class="btn-success" onclick="saveRecord()">Save</button>
            </div>
        </div>
    </div>

    <script>
        let state = { dbType: 'mongodb', connString: '', currentDb: null, currentCol: null, editingId: null };
        
        function updateBuilderDefaults() {
            const type = document.getElementById('b-type').value;
            const portInput = document.getElementById('b-port');
            if (type === 'mongodb') { portInput.value = '27017'; } else { portInput.value = '5432'; }
            buildString();
        }
        function buildString() {
            const type = document.getElementById('b-type').value;
            const host = document.getElementById('b-host').value || 'localhost';
            const port = document.getElementById('b-port').value;
            const user = document.getElementById('b-user').value;
            const pass = document.getElementById('b-pass').value;
            const safeUser = encodeURIComponent(user);
            const safePass = encodeURIComponent(pass);
            const auth = (user && pass) ? `${safeUser}:${safePass}@` : '';
            document.getElementById('conn-string').value = (type === 'mongodb') ? `mongodb://${auth}${host}:${port}` : `postgresql://${auth}${host}:${port}`;
            state.dbType = type;
        }
        buildString();

        function showStatus(msg, type='success') {
            const el = document.getElementById('status');
            el.textContent = msg; el.className = 'status ' + type; el.style.display = 'block';
            setTimeout(() => el.style.display = 'none', 4000);
        }
        function closeModals() { document.querySelectorAll('.modal').forEach(m => m.style.display = 'none'); }
        
        function openModal(type) { 
            closeModals(); 
            if (type === 'col') {
                document.getElementById('new-col-name').value = '';
                const pgBuilder = document.getElementById('pg-schema-builder');
                const label = document.getElementById('new-type-label');
                if (state.dbType === 'postgresql') {
                    pgBuilder.style.display = 'block'; label.textContent = 'Table';
                    document.getElementById('schema-rows').innerHTML = '';
                    addSchemaRow('id', 'SERIAL', true, true);
                    addSchemaRow('name', 'TEXT');
                    updateSqlPreview();
                } else {
                    pgBuilder.style.display = 'none'; label.textContent = 'Collection';
                }
            }
            document.getElementById('modal-' + type).style.display = 'block'; 
        }

        async function api(endpoint, method, data={}) {
            data.db_type = state.dbType;
            data.connection_string = document.getElementById('conn-string').value; 
            data.database = state.currentDb;
            try {
                const res = await fetch(endpoint, { method: method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data) });
                const json = await res.json();
                if (!res.ok) throw new Error(json.error || 'Unknown error');
                return json;
            } catch (e) { showStatus(e.message, 'error'); throw e; }
        }

        function addSchemaRow(name='', type='TEXT', pk=false, nn=false) {
            const container = document.getElementById('schema-rows');
            const div = document.createElement('div');
            div.className = 'schema-row';
            div.innerHTML = `
                <input placeholder="Column Name" class="col-name" value="${name}" oninput="updateSqlPreview()">
                <select class="col-type" onchange="updateSqlPreview()">
                    <option value="TEXT" ${type=='TEXT'?'selected':''}>TEXT</option>
                    <option value="INTEGER" ${type=='INTEGER'?'selected':''}>INTEGER</option>
                    <option value="SERIAL" ${type=='SERIAL'?'selected':''}>SERIAL (Auto ID)</option>
                    <option value="BOOLEAN" ${type=='BOOLEAN'?'selected':''}>BOOLEAN</option>
                    <option value="TIMESTAMP" ${type=='TIMESTAMP'?'selected':''}>TIMESTAMP</option>
                    <option value="DATE" ${type=='DATE'?'selected':''}>DATE</option>
                    <option value="JSONB" ${type=='JSONB'?'selected':''}>JSONB</option>
                    <option value="VARCHAR(255)" ${type.includes('VARCHAR')?'selected':''}>VARCHAR(255)</option>
                </select>
                <div style="text-align:center"><input type="checkbox" class="col-pk" ${pk?'checked':''} onchange="updateSqlPreview()"></div>
                <div style="text-align:center"><input type="checkbox" class="col-nn" ${nn?'checked':''} onchange="updateSqlPreview()"></div>
                <div style="text-align:center"><input type="checkbox" class="col-un" onchange="updateSqlPreview()"></div>
                <button class="btn-danger" style="padding: 2px 6px; font-size: 10px;" onclick="this.parentElement.remove(); updateSqlPreview()">X</button>
            `;
            container.appendChild(div);
            updateSqlPreview();
        }

        function updateSqlPreview() {
            const rows = document.querySelectorAll('.schema-row');
            let cols = [];
            rows.forEach(r => {
                const name = r.querySelector('.col-name').value.trim();
                const type = r.querySelector('.col-type').value;
                const pk = r.querySelector('.col-pk').checked;
                const nn = r.querySelector('.col-nn').checked;
                const un = r.querySelector('.col-un').checked;
                if (name) {
                    let def = `${name} ${type}`;
                    if (pk) def += ' PRIMARY KEY';
                    if (nn && !pk) def += ' NOT NULL'; 
                    if (un && !pk) def += ' UNIQUE';
                    cols.push(def);
                }
            });
            if (cols.length === 0) { document.getElementById('sql-preview').value = "-- Add columns to generate SQL --"; } 
            else { document.getElementById('sql-preview').value = cols.join(', '); }
        }

        async function createCollection() {
            const name = document.getElementById('new-col-name').value;
            if(!name) return showStatus("Name required", "error");
            let schema = null;
            if (state.dbType === 'postgresql') {
                updateSqlPreview();
                schema = document.getElementById('sql-preview').value;
                if (!schema || schema.startsWith('--')) schema = null; 
            }
            await api('/api/collection/create', 'POST', { collection_name: name, schema: schema });
            closeModals(); loadCollections();
        }

        async function openRecordModal(recordJson = null) {
            state.editingId = null;
            const container = document.getElementById('form-container');
            container.innerHTML = '<div style="color:#666; font-style:italic;">Loading schema...</div>';
            document.getElementById('record-modal-title').textContent = recordJson ? 'Edit Record' : 'New Record';
            openModal('record');

            let schema = [];
            let recordData = recordJson ? JSON.parse(recordJson) : {};
            
            if (state.dbType === 'postgresql') {
                const colData = await api('/api/collection/schema', 'POST', { collection_name: state.currentCol });
                schema = colData.map(c => c.name);
            }

            container.innerHTML = ''; 
            
            if (schema.length > 0) {
                schema.forEach(col => {
                    if (col === 'created_at') return; 
                    let val = recordData[col];
                    if (val === null || val === undefined) val = '';
                    
                    const div = document.createElement('div');
                    div.className = 'dynamic-field';
                    div.innerHTML = `
                        <label>${col}</label>
                        <input type="text" name="${col}" value="${val}">
                    `;
                    container.appendChild(div);
                });
                document.getElementById('add-field-btn').style.display = 'none';
            } else {
                const keys = Object.keys(recordData).filter(k => k !== '_id' && k !== 'id');
                if (keys.length === 0 && !recordJson) {
                    addKvField();
                } else {
                    keys.forEach(k => addKvField(k, recordData[k] === null ? '' : recordData[k]));
                }
                document.getElementById('add-field-btn').style.display = 'block';
            }
            
            if (recordJson) {
                state.editingId = recordData._id || recordData.id;
            }
        }

        function addKvField(key='', val='') {
            const container = document.getElementById('form-container');
            const div = document.createElement('div');
            div.className = 'dynamic-field kv-row';
            div.innerHTML = `
                <div class="field-row">
                    <input type="text" placeholder="Field Name" class="kv-key" value="${key}" style="flex:1">
                    <input type="text" placeholder="Value" class="kv-val" value="${val}" style="flex:2">
                    <button class="btn-danger" style="padding:5px 10px;" onclick="this.parentElement.parentElement.remove()">X</button>
                </div>
            `;
            container.appendChild(div);
        }

        async function saveRecord() {
            const data = {};
            const container = document.getElementById('form-container');
            const namedInputs = container.querySelectorAll('input:not(.kv-key):not(.kv-val)');
            if (namedInputs.length > 0) {
                namedInputs.forEach(input => {
                    if (input.name) {
                        data[input.name] = input.value === '' ? null : input.value;
                    }
                });
            } else {
                const rows = container.querySelectorAll('.kv-row');
                rows.forEach(row => {
                    const k = row.querySelector('.kv-key').value;
                    const v = row.querySelector('.kv-val').value;
                    if (k) data[k] = v === '' ? null : v;
                });
            }
            try {
                if (state.editingId) { await api('/api/records/' + state.editingId, 'PUT', { collection: state.currentCol, data }); } 
                else { await api('/api/records', 'POST', { collection: state.currentCol, data }); }
                closeModals(); loadRecords(); showStatus('Record saved');
            } catch (e) { showStatus('Server Error: ' + e.message, 'error'); }
        }

        async function connectToDatabase() {
            state.connString = document.getElementById('conn-string').value;
            state.dbType = document.getElementById('b-type').value;
            try {
                const data = await api('/api/connect', 'POST');
                const list = document.getElementById('db-list');
                list.innerHTML = '';
                if (data.databases.length === 0) list.innerHTML = '<span style="color:#666">No databases found (or access denied)</span>';
                data.databases.forEach(db => {
                    const chip = document.createElement('div');
                    chip.className = 'chip'; chip.textContent = db;
                    chip.onclick = () => selectDb(db, chip);
                    list.appendChild(chip);
                });
                document.getElementById('db-section').style.display = 'block';
                showStatus('Connected successfully!');
            } catch(e) {}
        }
        async function selectDb(name, el) {
            state.currentDb = name;
            document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
            el.classList.add('active');
            document.getElementById('main-interface').style.opacity = '1';
            document.getElementById('main-interface').style.pointerEvents = 'all';
            loadCollections();
        }
        async function loadCollections() {
            const cols = await api('/api/collections', 'POST');
            const list = document.getElementById('col-list');
            list.innerHTML = '';
            cols.forEach(c => {
                const div = document.createElement('div');
                div.className = 'collection-item'; div.textContent = c;
                div.onclick = () => {
                    state.currentCol = c;
                    document.querySelectorAll('.collection-item').forEach(x => x.classList.remove('active'));
                    div.classList.add('active');
                    loadRecords();
                };
                list.appendChild(div);
            });
        }
        async function loadRecords() {
            if (!state.currentCol) return;
            const records = await api('/api/records/list', 'POST', { collection: state.currentCol });
            const view = document.getElementById('data-view');
            if (!records.length) { view.innerHTML = '<div style="padding:40px; text-align:center; color:#888">No records found</div>'; return; }
            
            // Calculate Superset of Keys for MongoDB Heterogeneous Docs
            const allKeys = new Set();
            records.forEach(r => Object.keys(r).forEach(k => allKeys.add(k)));
            const keys = Array.from(allKeys).filter(k => k !== '_pg_ctid' && k !== 'id' && k !== '_id');
            
            let html = '<div style="display:inline-block; min-width:100%;"><table><thead><tr>';
            keys.forEach(k => html += `<th>${k}</th>`);
            html += '<th style="width:100px">Actions</th></tr></thead><tbody>';
            records.forEach(r => {
                html += '<tr>';
                keys.forEach(k => {
                    let val = r[k];
                    if (typeof val === 'object' && val !== null) val = JSON.stringify(val);
                    html += `<td>${val === undefined ? '' : val}</td>`;
                });
                const safeRecord = JSON.stringify(r).replace(/"/g, '&quot;');
                html += `<td><button class="btn-warning" style="padding:4px 8px; font-size:11px" onclick="openRecordModal('${safeRecord}')">Edit</button> <button class="btn-danger" style="padding:4px 8px; font-size:11px" onclick="deleteRecord('${r.id || r._id}')">Del</button></td></tr>`;
            });
            html += '</tbody></table></div>';
            view.innerHTML = html;
        }
        async function deleteRecord(id) {
            if(!confirm('Delete this record?')) return;
            await api('/api/records/' + id, 'DELETE', { collection: state.currentCol });
            loadRecords(); showStatus('Deleted');
        }
        async function createDatabase() {
            const name = document.getElementById('new-db-name').value;
            if(!name) return;
            await api('/api/database/create', 'POST', { database_name: name });
            closeModals(); connectToDatabase(); 
        }
        async function dropDatabase() {
            if(!confirm('WARNING: PERMANENTLY DELETE DATABASE ' + state.currentDb + '?')) return;
            await api('/api/database/drop', 'DELETE', { database_name: state.currentDb });
            location.reload();
        }
        async function dropCollection() {
            if(!confirm('Delete table ' + state.currentCol + '?')) return;
            await api('/api/collection/drop', 'DELETE', { collection_name: state.currentCol });
            state.currentCol = null; loadCollections(); document.getElementById('data-view').innerHTML = '<div style="padding:20px; text-align:center; color:#999">Select a collection</div>';
        }
    </script>
</body>
</html>
"""

# --- ROUTES ---
@app.route('/')
def index(): return render_template_string(HTML_TEMPLATE)

def get_db(data):
    db = DatabaseManager(data['db_type'], data['connection_string'])
    if data.get('database'):
        if db.db_type == 'mongodb':
            client = MongoClient(db.connection_string, serverSelectionTimeoutMS=5000)
            db.connection = client[data['database']]
        else:
            conn_str = db._get_postgres_conn_string(data['database'])
            db.connection = psycopg2.connect(conn_str)
    else: db.connect()
    return db

@app.route('/api/connect', methods=['POST'])
def r_connect():
    try:
        db = DatabaseManager(request.json['db_type'], request.json['connection_string'])
        return jsonify({'databases': db.get_databases()})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/collections', methods=['POST'])
def r_cols():
    try: return jsonify(get_db(request.json).get_collections_or_tables())
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/collection/schema', methods=['POST'])
def r_schema():
    try:
        db = get_db(request.json)
        return jsonify(db.get_table_columns(request.json['collection_name']))
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/records/list', methods=['POST'])
def r_list():
    try: return jsonify(get_db(request.json).get_all_records(request.json['collection']))
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/records', methods=['POST'])
def r_create():
    try: return jsonify({'id': get_db(request.json).create_record(request.json['collection'], request.json['data'])})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/records/<id>', methods=['PUT'])
def r_update(id):
    try: return jsonify({'success': get_db(request.json).update_record(request.json['collection'], id, request.json['data'])})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/records/<id>', methods=['DELETE'])
def r_delete(id):
    try: return jsonify({'success': get_db(request.json).delete_record(request.json['collection'], id)})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/database/create', methods=['POST'])
def r_db_create():
    try:
        DatabaseManager(request.json['db_type'], request.json['connection_string']).create_database(request.json['database_name'])
        return jsonify({'success': True})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/database/drop', methods=['DELETE'])
def r_db_drop():
    try:
        DatabaseManager(request.json['db_type'], request.json['connection_string']).drop_database(request.json['database_name'])
        return jsonify({'success': True})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/collection/create', methods=['POST'])
def r_col_create():
    try:
        get_db(request.json).create_collection(request.json['collection_name'], request.json.get('schema'))
        return jsonify({'success': True})
    except Exception as e: return jsonify({'error': str(e)}), 400

@app.route('/api/collection/drop', methods=['DELETE'])
def r_col_drop():
    try:
        get_db(request.json).drop_collection(request.json['collection_name'])
        return jsonify({'success': True})
    except Exception as e: return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=8000)
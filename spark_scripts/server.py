"""
server.py - VERSION FINALE
Flask + Socket.IO — pont entre Spark et le navigateur.
Aucun changement fonctionnel, juste nettoyé.
"""

from flask import Flask
from flask_socketio import SocketIO, emit
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'surveillance_bigdata_2025'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading',
                    logger=False, engineio_logger=False)

historique_alertes = []
MAX_HISTORIQUE = 100
etat_patients = {}

print("[SERVER] Flask + Socket.IO démarré sur http://0.0.0.0:5000")

@app.route('/')
def index():
    path = os.path.join(os.path.dirname(__file__), 'dashboard.html')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    return "<h2>dashboard.html introuvable</h2>", 404

@socketio.on('connect')
def on_connect():
    print("[SERVER] Client connecté")
    for alerte in historique_alertes[-20:]:
        emit('alerte', alerte)
    for pid, etat in etat_patients.items():
        emit('patient_update', etat)

@socketio.on('disconnect')
def on_disconnect():
    print("[SERVER] Client déconnecté")

@socketio.on('alerte')
def on_alerte(data):
    global historique_alertes
    historique_alertes.append(data)
    if len(historique_alertes) > MAX_HISTORIQUE:
        historique_alertes = historique_alertes[-MAX_HISTORIQUE:]
    socketio.emit('alerte', data, broadcast=True)
    print(f"[SERVER] Alerte → {data.get('patient_id')} | "
          f"{data.get('signal')} = {data.get('valeur_mesuree')} | "
          f"{data.get('niveau_alerte')}")

@socketio.on('patient_update')
def on_patient_update(data):
    pid = data.get('patient_id')
    etat_patients[pid] = data
    socketio.emit('patient_update', data, broadcast=True)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)

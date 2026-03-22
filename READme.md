# 🚀 Innovazione del progetto *final_project*

Questa repository propone una versione più automatizzata ed estesa del progetto originale: https://github.com/edoardo-belingheri/final_project.  
L’intero ciclo di vita dei dati viene gestito end‑to‑end: estrazione, pulizia, trasformazione, analisi esplorativa e sviluppo di una dashboard interattiva.

---

## 🎯 Obiettivo del progetto

L’obiettivo principale è **automatizzare la pipeline dei dati**, riducendo l’intervento manuale e garantendo **riproducibilità**, **tracciabilità** e **qualità** tramite log sistematici.  
Il risultato è un flusso aggiornabile in modo semplice, rapido e affidabile.

Gli obiettivi originali rimangono invariati:
1. Estrarre i dati dal database pubblico tramite SQL  
2. Pulire e trasformare i dataset  
3. Effettuare un’analisi esplorativa completa  
4. Costruire una dashboard interattiva in Power BI  

---

## 📥 Estrazione automatizzata da BigQuery

La componente centrale dell’automazione è lo script `a)_importazione_dati_sql/importazione.py`, che sostituisce l’esportazione manuale delle query SQL e recupera i dati direttamente da Google BigQuery.

### Come funziona

- **Connessione** — Inizializza un client BigQuery autenticato.  
- **Estrazione batch** — Esegue tutte le query definite nel dizionario `QUERIES`, coprendo ogni tabella necessaria.  
- **Persistenza** — Salva i risultati in formato `.csv` nella cartella `f)_data/initial_data/`, pronti per la fase di pulizia.

### 🔑 Autenticazione

Per eseguire lo script è necessario autenticare l’ambiente Google Cloud:

- `gcloud auth application-default login`  
- oppure impostare la variabile d’ambiente `GOOGLE_APPLICATION_CREDENTIALS` con il percorso del file JSON della Service Account  

---

## 🛠 Pulizia, trasformazione e controllo qualità

La logica principale della pipeline è contenuta in `caricamento_e_pulizia.py`, che trasforma i dati grezzi in un dataset pulito, arricchito e pronto per analisi avanzate o dashboard BI.

### Funzionalità principali

- **Normalizzazione** — Uniforma i formati temporali (UTC, rimozione millisecondi) per evitare inconsistenze.  
- **Feature Engineering** — Genera nuove variabili utili all’analisi:  
  - *Giacenza*: giorni a scaffale e flag `in_stock`  
  - *Redditività*: margine netto considerando resi e cancellazioni  
  - *Segmentazione*: classificazione automatica degli utenti per fasce d’età  
- **Data Quality & Logging** — Ogni esecuzione produce un log in `/logs` con l’output di `df.info()`, utile per monitorare struttura, tipi e valori nulli nel tempo.  
- **Persistenza finale** — I dataset puliti vengono salvati in `f)_data/clean_data/` in formato CSV.  

---

## 🔍 Analisi Esplorativa

L’EDA ha permesso di comprendere:

- il comportamento di navigazione (website activity)
- la composizione demografica degli utenti
- la distribuzione geografica del traffico  
- la performance dei prodotti e delle categorie  
- la situazione dell’inventario e dei distribution centers  
- i pattern degli ordini e della revenue  

Gli insight sono disponibili nel notebook `c)_eda/eda.ipynb`.

---

## 🧪 Test d’Ipotesi e Analisi di Regressione

Nel progetto è stata inclusa una sezione dedicata all’analisi statistica, con l’obiettivo di valutare in modo rigoroso le differenze di performance tra mercati (Giapponese e Coreano) e supportare decisioni di business basate sui dati.  
L’analisi comprende un test d’ipotesi non parametrico e un modello di regressione, utilizzati per verificare la significatività delle differenze osservate.

metodologia, codice, visualizzazioni e interpretazione — è disponibile nel notebook dedicato:

`d)_test_statistici/ipotesi_e_regressione.ipynb`

---

## 📊 Dashboard in Power BI

La rivisitazione della dashboard finale include sempre:

- KPI principali (profit, conversion rate, revenue)
- segmentazioni per età, genere e comportamento utente 
- analisi per Paese e canale  
- performance dei prodotti  
- stato dell’inventario e caratteristiche dei distribution centers  
 
Il file `dashboard.pbix` è disponibile nella cartella `e)_dashboard/`.

---

## 🎤 Presentazione & Data Storytelling

Per accompagnare l’analisi tecnica è stata realizzata una **presentazione dedicata ai principali insight**, con l’obiettivo di comunicare i risultati in modo chiaro, sintetico e orientato alle decisioni.

La presentazione segue i principi fondamentali del **data storytelling**:

- **Contesto prima dei numeri** — ogni insight è introdotto dal perché è rilevante per il business  
- **Messaggi chiave evidenziati** — ogni slide contiene un’unica idea centrale, supportata dai dati  
- **Visual puliti e leggibili** — grafici essenziali, colori coerenti e gerarchie visive chiare    

La presentazione è disponibile nella cartella `h)_presentazione/`.

# GA · IBKR Tick Collector

Collector locale in sola lettura per EURUSD spot (`EUR/CASH/USD/IDEALPRO`).
Login e 2FA si completano manualmente in IB Gateway Paper Trading.
Il codice non invia ordini. k3s è fuori dal perimetro della versione 0.1.

## Installazione in VS Code (PowerShell)

Aprire questa root e scegliere un interprete Python >= 3.11:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
Copy-Item config.example.toml config.toml
```

Scaricare l'SDK **ufficiale TWS API Latest 1052.01** dalla
[pagina IBKR](https://download2.interactivebrokers.com/installers/tws-api/index.html).
La pagina attuale indica Python incluso in Latest e non in Stable 1050.02.
Installare/estrarre il download e installare il suo `source/pythonclient`
nell'ambiente del progetto, adattando il percorso:

```powershell
python -m pip install "C:\TWS API\source\pythonclient"
python -m pip show ibapi
```

Seguire le [istruzioni IBKR](https://www.interactivebrokers.com/campus/trading-lessons/accessing-the-tws-python-api-source-code/).
Non sostituire il download ufficiale con il vecchio pacchetto omonimo PyPI.
SDK di riferimento: 10.52.01; compatibilità effettiva SDK/Gateway da confermare
con lo smoke test. I test offline non richiedono ibapi.

## Avvio

Nel Gateway scegliere **Paper Trading**, completare login/2FA, abilitare socket
API, attivare **Read-Only API**, consentire localhost e verificare la porta
(iniziale: 4002). Usare un client ID dedicato senza client concorrenti.

```powershell
ibkr-ticks collect --config config.toml
# Ctrl+C: disconnessione, svuotamento coda e chiusura segmento
ibkr-ticks compact --config config.toml
# Solo per eliminare gli originali dopo verifica integrale del Parquet:
ibkr-ticks compact --config config.toml --delete-originals
python -m pytest -q
```

SIGTERM esegue la stessa chiusura ordinata; la terminazione forzata su Windows
non equivale a SIGTERM. Errori: exit code 1. La configurazione valida tipi,
soglie, contratto e opzioni sconosciute. `storage.directory` è relativo al TOML:
con l'esempio i dati sono in `data/`. Configurazioni locali, dati, ambiente e
`logs/` sono esclusi da Git. Log su console in UTC: conteggi ricevuti/scritti,
backlog, spazio, interruzioni ed errori. Per conservarli:

```powershell
New-Item -ItemType Directory -Force logs
ibkr-ticks collect --config config.toml 2> logs/collector.log
```

## Dati e significato

Ogni callback BidAsk genera una riga, anche quando tutti i valori sono identici.
`ignoreSize=False` include i cambi della sola size secondo la
[documentazione IBKR](https://www.interactivebrokers.com/docs/tws-api/doc/md/live-ticks).
Le size sono **quantità quotate**, non volume effettivamente scambiato.
Il conteggio degli aggiornamenti per intervallo misura attività del feed,
non volume di scambi. `bid_size` e `ask_size` sono stringhe decimali anche in
Parquet: nessun limite di scala o arrotondamento aggiunto dal collector.

Campi: `contract`, `instrument`, `source`, `ibkr_timestamp` (secondi Unix
originali), `received_utc_ns` (UTC locale in nanosecondi), `run_id`, `sequence`
(da 1 per avvio), `bid`, `ask`, `bid_size`, `ask_size`,
`attributes.bidPastLow`, `attributes.askPastHigh`.
L'ordine autorevole per avvio è `sequence`; l'orologio locale può essere
corretto dal sistema. I file Parquet non hanno ordine globale implicito.

## Connessione e affidabilità

Solo l'Adapter importa EClient/EWrapper. Dopo `nextValidId` richiede
`reqTickByTickData(1, contract, "BidAsk", 0, False)`.
Il callback usa `put_nowait` su coda limitata; un solo thread scrive.
Se la coda satura, ferma la raccolta, segnala la sequenza persa e svuota gli
eventi già accodati. La raccolta viene dichiarata incompleta.
Le connessioni interrotte ritentano con backoff 1–60 secondi e timeout handshake
di 15 secondi. Per ogni ciclo 1100/1101 risottoscrive una volta; 1102 mantiene
la sottoscrizione, secondo i [codici IBKR](https://www.interactivebrokers.com/docs/tws-api/doc/error/codes).
`ibkr.tick_request_min_interval_seconds` (default 15) governa esclusivamente
le richieste di sottoscrizione sullo stesso strumento, non la frequenza dei tick:
una sottoscrizione attiva continua a ricevere aggiornamenti alla frequenza del feed.
Ogni numero finito non negativo è accettato e applicato senza imporre un minimo;
valori inferiori a 15 secondi possono causare un rifiuto IBKR per limiti di pacing.
La prima richiesta parte dopo l'handshake; le successive restano pendenti fino
alla scadenza monotona, anche dopo 1101 o ricreazione dell'Adapter. Durante
l'attesa restano attivi controllo disco, riconnessione e arresto. Il timeout
dell'handshake è distinto da questa attesa. I log distinguono l'invio della
richiesta (`Sending BidAsk subscription request`) dal primo callback ricevuto
(`First BidAsk tick received`): l'invio non conferma la disponibilità del feed.
I codici 2103 (farm market data), 2105 (farm storica) e 2110 (connessione
Gateway/server) sono interruzioni recuperabili secondo la stessa documentazione
ufficiale: vengono registrati e si attende il recupero IBKR senza arrestare
il collector. Errori di contratto o permessi, ad esempio 200 e 354, restano fatali.
Le lacune durante le disconnessioni non vengono recuperate da storico.
Altri errori non informativi arrestano il processo rendendo visibili problemi
di contratto e permessi. La soglia disco viene controllata ogni
`status_interval_seconds`: lasciare margine per coda e altri processi.

## Segmenti e compattazione

`data/segments/*.open.ndjson` identifica i segmenti aperti. Flush e fsync
periodici limitano le perdite dopo crash. Durata o dimensione chiudono i file
come `.closed.ndjson`; una singola riga può superare il limite byte.
All'avvio di collect, sotto lock esclusivo, le righe complete degli aperti
vengono validate e conservate, l'ultima riga parziale segnalata e troncata.
Una riga completa corrotta provoca errore. Buffer e coda possono andare persi
in un crash: non si promette raccolta senza perdite.

Collect e compact possono girare contemporaneamente: `.writer.lock` consente
un solo collector (incluso il recupero) e `.converter.lock` un solo convertitore
per directory dati. I lock OS sono rilasciati anche dopo crash. Il writer modifica
solo gli aperti e pubblica i chiusi con rename atomico, anche durante il recupero.
I chiusi sono immutabili; compact non legge gli aperti. Compact legge solo i chiusi e partiziona
per strumento e data UTC di ricezione in
`data/parquet/instrument=EUR_CASH_USD_IDEALPRO/date=YYYY-MM-DD/`.
Ogni nome output deriva dall'identità e dal contenuto del segmento: un retry
verifica i file già presenti senza aggiungere eventi. Zstandard, file temporaneo,
confronto dello schema e di ogni record, fsync e rename atomico precedono la
pubblicazione. I temporanei vengono sovrascritti al retry. Gli originali restano
salvo `--delete-originals`, che li elimina solo dopo la verifica di tutte le
partizioni corrispondenti. Non modificare segmenti già compattati.
La memoria di compact è proporzionale alla dimensione del segmento.

Lettura del Parquet senza pandas, con precisione delle size:

```python
from pathlib import Path
from decimal import Decimal
import pyarrow.parquet as pq

rows = [r for p in Path("data/parquet").rglob("*.parquet")
        for r in pq.ParquetFile(p).read().to_pylist()]
rows.sort(key=lambda r: (r["run_id"], r["sequence"]))
for r in rows[:5]:
    print(r["ibkr_timestamp"], r["bid"], Decimal(r["bid_size"]),
          r["ask"], Decimal(r["ask_size"]))
```

## Verifica offline e smoke test Paper

`python -m pytest -q` esegue la pipeline simulata: cambi della sola size,
eventi identici, precisione decimale, ordine, drain, rotazione, recupero dopo
interruzione, riconnessione, 1101/1102 ripetuti, confronto integrale NDJSON/Parquet,
conversione ripetuta e rimozione esplicita. Copre inoltre output corrotti,
saturazione, soglia disco, errore writer, lock, TOML e chiamata SDK con un doppio.
I test di regressione caricano il vero TOML di esempio dalla CLI in una directory
temporanea e usano un clock simulato per pacing a 5, 15 e 30 secondi, sia dopo
1101 sia dopo caduta socket, inclusi arresto durante l'attesa e 1102 ripetuti.
Verificano anche compact mentre il collector tiene aperto il segmento successivo.

Per ripetere manualmente la prova CLI senza scrivere dati nella root:

```powershell
$exampleCheck = Join-Path ([System.IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
New-Item -ItemType Directory $exampleCheck
Copy-Item config.example.toml (Join-Path $exampleCheck 'config.example.toml')
ibkr-ticks compact --config (Join-Path $exampleCheck 'config.example.toml')
ibkr-ticks compact --config (Join-Path $exampleCheck 'config.example.toml')
```

Questa prova del comando su directory vuota verifica il TOML e l'avvio della
CLI; la suite verifica anche conversione e confronto dei record simulati.

Smoke test da eseguire sulla macchina con Gateway:

1. Copiare il TOML in `config.paper-smoke.toml`, impostare directory
   `data/paper-smoke` e annotare versioni SDK/Gateway e permessi market data.
2. Durante il mercato FX aperto eseguire
   `ibkr-ticks collect --config config.paper-smoke.toml`. Verificare handshake,
   `Sending BidAsk subscription request`, `First BidAsk tick received`, contatori
   crescenti e assenza di errori permessi.
   La sola sottoscrizione non dimostra arrivo di callback.
3. Dopo almeno un minuto fermare con Ctrl+C. Nei segmenti chiusi verificare
   `IBKR.tickByTickBidAsk`, contratto EURUSD, timestamp, bid/ask e presenza di
   entrambe le size. Confrontare con il Gateway e cercare un cambio della sola
   size, se fornito dal feed. Assenza di callback non dimostra mercato inattivo.
4. Compattare due volte con la configurazione smoke, leggere con l'esempio e
   confrontare conteggio e record con NDJSON.
5. Tenere questi dati separati finché qualità e disponibilità del feed Paper
   non sono confermate; i permessi dipendono dal conto e dalle sottoscrizioni.

In questo ambiente non è stato collegato un Gateway Paper: installazione SDK
ufficiale e smoke test reale restano da eseguire sulla macchina utente.

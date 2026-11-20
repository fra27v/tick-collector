# Specifica della prima versione

Implementare un collector Python locale con due comandi, `collect` e `compact`.
Usare l'SDK ufficiale `ibapi` della TWS API. Documentare l'installazione della
versione scelta e lo smoke test su IB Gateway in modalità Paper Trading.
Login ed eventuale 2FA si completano nel Gateway. La prima versione è in
sola lettura. Il deployment k3s verrà affrontato dopo la prova locale.

## Acquisizione

- Usare EURUSD spot (`EUR`, `CASH`, `USD`, `IDEALPRO`). Dopo l'handshake
  sottoscrivere `reqTickByTickData` con `BidAsk`, `numberOfTicks=0` e
  `ignoreSize=False`.
- Salvare contratto, fonte, timestamp IBKR in secondi Unix, timestamp UTC di
  ricezione in nanosecondi, ID dell'avvio, sequenza, bid, ask, bid size, ask size
  e attributi del callback. Preservare le size decimali, gli aggiornamenti della
  sola size e gli eventi con valori identici. Le size sono quantità quotate.
  Il conteggio delle quotazioni per intervallo misura l'attività del feed.
- Usare un Adapter per isolare `EClient` e `EWrapper`. Separare connessione,
  modello del tick e scrittura. Caricare e validare la configurazione TOML.
- Usare una coda limitata e un solo writer. Registrare interruzioni, saturazione
  ed errori disco. Gestire 1101 e 1102 senza sottoscrizioni duplicate.
  Ritentare la connessione con backoff e svuotare la coda su Ctrl+C o SIGTERM.

## Storage

- Scrivere segmenti NDJSON a rotazione per durata o dimensione. Distinguere
  segmenti aperti e chiusi. Recuperare le righe complete dopo un arresto
  improvviso e segnalare una coda incompleta.
- Convertire solo i segmenti chiusi in Parquet con Zstandard, partizionato per
  strumento e giorno UTC di ricezione. Verificare schema e contenuto, pubblicare
  con rename atomico e rendere i retry idempotenti. Rimuovere gli originali
  verificati solo su richiesta esplicita.
- Riportare periodicamente conteggi, backlog e spazio libero. Fermare la
  raccolta in modo ordinato alla soglia disco critica.

## Verifica

Eseguire test offline con feed simulato per aggiornamenti della sola size,
eventi identici, recupero dopo arresto, riconnessione e conversione ripetibile.
Verificare che tutti gli eventi attesi siano leggibili dal Parquet. Documentare
come verificare sul conto paper la disponibilità di BidAsk e delle size,
tenendo separati i dati di una prova con qualità del feed incerta.

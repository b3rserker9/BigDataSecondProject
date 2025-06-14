# Used Cars Big Data Project

Questo progetto è stato realizzato come parte di un esame universitario per il corso di Big Data Analytics.

L’obiettivo è sviluppare e analizzare diversi job di Big Data utilizzando tecnologie quali Hive, SparkSQL e MapReduce, per generare report dettagliati su modelli di auto usate, suddivisi per città, anno e fasce di prezzo.

## Dataset utilizzato

Il dataset usato è il **"US Used Cars Dataset"**, reperito da [Kaggle](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset) e utilizzato per eseguire tutte le analisi e le simulazioni.

## Job implementati

- **JOB1:** Generazione di statistiche per ogni marca di auto (`make_name`) con dettaglio dei modelli, conteggio, prezzi (min, max, medio) e anni di presenza.
- **JOB2:** Report per ogni città e anno con suddivisione in fasce di prezzo (alto, medio, basso), con numero di auto, media dei giorni sul mercato e le 3 parole più frequenti nelle descrizioni.

Tutti i job sono stati progettati per essere eseguiti su un cluster Hadoop, sfruttando la potenza del processamento distribuito.

## Repository

Nel repository sono presenti:
- Codice sorgente per Hive, SparkSQL e MapReduce.
- Script di preparazione e suddivisione del dataset.
- Dati utilizzati per le simulazioni.

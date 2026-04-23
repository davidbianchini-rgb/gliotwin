# Gliotwin LLM Guide

## Scopo del documento

Questo file serve come riferimento operativo per agenti di coding LLM come Codex, Claude Code e strumenti simili.

Non e' documentazione per utenti finali.
Non e' una specifica clinica.
Non e' una descrizione generale del progetto scritta per umani.

Lo scopo e' fissare in modo esplicito:

- il modello mentale corretto del progetto
- la separazione logica delle fasi
- il lessico da usare in modo consistente
- i vincoli da rispettare quando si modifica il codice
- la differenza tra cio' che esiste gia' e cio' che e' ancora da progettare

## Regola generale per gli LLM

Quando si lavora su Gliotwin, non trattare il sistema come una singola pipeline monolitica.

Il progetto va interpretato come un percorso a stati applicato a:

`paziente -> timepoint -> sessione -> serie -> strutture`

Nota pratica:

- nel linguaggio del progetto il `timepoint` coincide operativamente con la `sessione` del paziente
- ogni timepoint/sessione deve poter avanzare lungo una checklist di passi
- ogni passo deve avere almeno uno stato `ok` oppure `non ok`
- se un passo e' `ok`, deve essere chiaro come e' stato completato
- se un passo e' `non ok`, deve essere chiaro quale problema blocca l'avanzamento

Il sistema deve poter selezionare i soggetti in base allo stato corrente e far avanzare tutti i casi pronti a uno specifico passo, invece di forzare sempre un'elaborazione end-to-end per singolo paziente.

## Principi architetturali da rispettare

### 1. Separazione logica delle fasi

Le fasi principali del progetto sono:

- `IMPORT`
- `PREPROCESSING`
- `SEGMENTAZIONE`
- `ANALISI`
- `EXPORT`

Queste fasi sono prima di tutto blocchi logici.
Possono essere implementate come moduli separati oppure no, ma nel codice non devono essere confuse tra loro.

### 2. Canonizzazione come standardizzazione

La canonizzazione non significa solo convertire file.
Significa standardizzare dati imaging, strutture e dati clinici provenienti da fonti diverse in una rappresentazione confrontabile.

La canonizzazione deve risolvere differenze tra dataset che impediscono:

- confronto tra soggetti
- confronto tra timepoint
- uso uniforme dei preprocessori
- uso uniforme dei segmentatori
- uso uniforme delle analisi

### 3. Gestione per stato

Ogni timepoint/sessione deve avere uno stato esplicito per ogni passo rilevante.
Gli LLM devono preferire strutture dati, API e UI che riflettano questo approccio a checklist.

### 4. Ambiguita' in revisione manuale

Le ambiguita' di identificazione o mapping non devono essere risolte in modo silenzioso.

Esempi:

- stesso ID con nomi diversi
- stesso nome con ID diversi
- serie imaging con classificazione incerta
- strutture con origine o riferimento non chiaro

In questi casi il caso deve entrare in revisione manuale.

### 5. Segmentazione come fase indipendente

La segmentazione non fa parte del preprocessing.
I motori di segmentazione sono consumatori di input canonici e produttori di strutture.

### 6. Evitare gerarchie inutili sulle strutture

Le strutture devono essere classificate soprattutto per origine, non tramite molte gerarchie artificiali.

Le origini possono stare sullo stesso piano logico, per esempio:

- importata da dataset sorgente
- calcolata da un motore specifico
- calcolata da un altro motore specifico
- creata manualmente

## Modello logico del dato

La gerarchia canonica e':

`paziente -> timepoint -> sessione -> serie -> strutture`

Indicazioni operative:

- `paziente`: identita' canonica del soggetto
- `timepoint/sessione`: istanza temporale del soggetto
- `serie`: immagini o acquisizioni riconosciute come unita' di lavoro
- `strutture`: ROI, maschere, RTSTRUCT convertite, segmentazioni, strutture derivate

I dati clinici fanno parte della canonizzazione del soggetto e del contesto temporale, ma `ANALISI` resta una fase separata.

## Fasi del progetto

## `IMPORT`

Responsabilita':

- recuperare i dati dalle sorgenti note
- leggere imaging, strutture e dati clinici
- applicare regole specifiche per dataset
- gestire eccezioni specifiche per dataset
- rendere i dati disponibili al `PREPROCESSING`

Vincoli:

- ogni dataset puo' avere regole proprie di import
- ogni dataset puo' avere eccezioni proprie
- anche le strutture hanno regole di import dedicate
- le ambiguita' devono andare in revisione manuale

Dataset sorgente da considerare come ufficiali:

- `IRST`
- `MU-Glioma-Post`
- `UCSD-PTGBM`
- `RHUH-GBM`
- `QIN-GBM-TREATMENT-RESPONSE`
- `GLIS-RT`

Indicazione di prodotto:

- questi nomi devono comparire in una tendina o selezione equivalente lato applicazione
- nel backend devono esistere le azioni e regole necessarie per l'import/conversione di ciascun dataset

## `PREPROCESSING`

Responsabilita':

- prendere dati riconosciuti e importati
- riconoscere soggetto, timepoint/sessione, sequenze richieste e strutture associate
- convertire il dato nel formato standard usabile dai segmentatori
- portare le immagini in uno spazio canonico utile a visualizzazione e segmentazione

Il preprocessing include, in termini logici:

- riconoscimento delle sequenze richieste
- gestione di regole dedicate per sequenze speciali come `APT`
- conversione immagini e strutture in `NIfTI` quando necessario
- registrazione e allineamento nello spazio canonico
- skull stripping o passaggi analoghi
- rimappatura/preparazione del segnale

Vincoli:

- il dato intermedio non deve essere necessariamente il centro dell'esperienza utente
- pero' il sistema deve permettere di eseguire i passaggi in serie e recuperare gli intermedi quando servono
- le trasformazioni da mantenere servono soprattutto per il ritorno delle strutture allo spazio nativo DICOM in fase di export
- al momento il caso d'uso rilevante riguarda trasformazioni rigide, non deformabili

Importante:

- non fissare come requisito forte dettagli non ancora verificati da test
- in particolare i vincoli finali dell'`EXPORT` devono diventare prescrittivi solo dopo verifica pratica

## `SEGMENTAZIONE`

Responsabilita':

- eseguire motori di segmentazione esterni o dedicati
- ricevere input canonici dal preprocessing
- produrre strutture coerenti con il modello dati del progetto

Vincoli:

- i motori di segmentazione sono moduli separati
- devono prendere input standardizzati e restituire strutture
- le strutture prodotte devono convivere con quelle importate o di altra origine

Nota di progetto:

- la generazione di nuove strutture tramite algebra di strutture esistenti e' prevista
- non e' una priorita' immediata
- non va forzata nel design attuale piu' del necessario

## `ANALISI`

Responsabilita':

- analisi longitudinali sul singolo paziente nel tempo
- analisi di coorte
- statistica
- selezione dei dati tramite filtri su soggetti, sequenze e strutture

Vincoli:

- `ANALISI` e' un menu/modulo separato da valutare meglio nel disegno frontend e backend
- usare l'impianto attuale come riferimento, senza irrigidire ora forme definitive

## `EXPORT`

Responsabilita':

- esportare le strutture in `DICOM RTSTRUCT`
- associare l'output a una serie nativa

Vincoli attuali di scrittura del codice:

- supportare solo `DICOM RTSTRUCT` su serie native
- non rendere ancora prescrittivi i dettagli finali della reversibilita' geometrica finche' non sono verificati con test end-to-end

## Checklist per timepoint/sessione

Ogni timepoint/sessione dovrebbe poter essere descritto da una checklist a stati.

Esempio logico minimo:

- dataset riconosciuto
- soggetto riconosciuto
- timepoint/sessione riconosciuto
- serie imaging riconosciute
- strutture riconosciute
- ambiguita' risolte oppure inviate a revisione manuale
- import completato
- preprocessing pronto
- preprocessing completato
- segmentazione eseguibile
- segmentazione completata
- analisi disponibile
- export disponibile

Per ogni elemento della checklist serve:

- stato
- eventuale errore/blocco
- traccia di come il passo e' stato completato

## Stato attuale osservato nel repository

Osservazioni utili per gli LLM, derivate dal codice presente:

- esiste gia' un database con tabelle per `subjects`, `sessions`, `sequences`, `radiological_structures`, `computed_structures`, `clinical_events`, `processing_jobs`
- esiste gia' il concetto di `session_label` e di `timepoint`
- esiste gia' uno stato di pipeline serializzato per singolo `subject/timepoint`
- esistono gia' pipeline di import dedicate per alcuni dataset sorgente
- esiste gia' una distinzione pratica tra strutture radiologiche e strutture computate

Questo significa che gli LLM devono partire dall'impianto esistente e non riscrivere il progetto da zero.

## Cosa e' ancora da progettare o chiarire meglio

Rispetto allo stato attuale, le aree ancora da definire meglio sono:

- formalizzazione completa della checklist unica per tutti i timepoint
- regole uniformi di avanzamento per stato e selezione batch dei casi pronti
- definizione completa delle regole di import per tutti i dataset ufficiali
- definizione completa delle regole di import per le strutture provenienti da sorgenti eterogenee
- standardizzazione esplicita dell'origine delle strutture in un modello semplice e uniforme
- definizione piu' precisa del modulo `ANALISI` lato frontend e backend
- definizione testata e verificata del ritorno delle strutture verso `DICOM RTSTRUCT`

## Indicazioni di comportamento per gli LLM

Quando modifichi il progetto:

- non confondere `IMPORT` con `PREPROCESSING`
- non confondere `PREPROCESSING` con `SEGMENTAZIONE`
- non introdurre gerarchie complesse sulle strutture senza necessita' reale
- tratta le ambiguita' come casi da revisione manuale
- progetta sempre in termini di avanzamento per stato del `timepoint/sessione`
- preserva la possibilita' di lavorare per selezione di soggetti e stati
- usa l'architettura attuale come base, non inventare un sistema totalmente diverso senza motivo
- se un vincolo di export o reversibilita' non e' ancora testato, non presentarlo come fatto acquisito

## In sintesi

Gliotwin non deve essere pensato come un semplice convertitore o come un singolo segmentatore.
Deve essere pensato come un sistema che standardizza dati eterogenei, li organizza per paziente e timepoint/sessione, li fa avanzare lungo una checklist di lavorazione e li rende utilizzabili per segmentazione, analisi longitudinale/coorte ed export finale.

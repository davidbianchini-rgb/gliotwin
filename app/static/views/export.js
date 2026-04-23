/* export.js — Export phase: DICOM RTSTRUCT output */
'use strict';

GlioTwin.register('export', async (app) => {
  app.innerHTML = `
    <div class="export-layout">
      <div class="export-header">
        <div>
          <div class="export-title">Export</div>
          <div class="export-subtitle">Esporta strutture computate o importate in formato DICOM RTSTRUCT, associate alla serie nativa di riferimento.</div>
        </div>
      </div>
      <div class="export-placeholder">
        <div class="export-placeholder-icon">↓</div>
        <div class="export-placeholder-title">Export — in sviluppo</div>
        <div class="export-placeholder-copy">
          Questa fase gestirà l'export delle strutture in <code>DICOM RTSTRUCT</code> sulla serie nativa.<br>
          La funzione richiede che il timepoint abbia completato la fase di segmentazione e che sia disponibile la trasformazione di ritorno verso lo spazio DICOM nativo.
        </div>
        <div class="export-checklist">
          <div class="export-check-item">
            <span class="export-check-icon pending">○</span>
            Selezione soggetto e timepoint
          </div>
          <div class="export-check-item">
            <span class="export-check-icon pending">○</span>
            Selezione strutture da esportare
          </div>
          <div class="export-check-item">
            <span class="export-check-icon pending">○</span>
            Selezione serie nativa di riferimento
          </div>
          <div class="export-check-item">
            <span class="export-check-icon pending">○</span>
            Generazione RTSTRUCT e download
          </div>
        </div>
      </div>
    </div>
  `;
});

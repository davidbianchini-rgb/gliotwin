/* ============================================================
   GlioTwin Viewer — NiiVue wrapper (multi-instance, per-cell)
   ============================================================ */

const GlioViewer = (() => {

  const CUSTOM_COLORMAPS = {
    glio_red: {
      R: [0, 239],
      G: [0, 68],
      B: [0, 68],
      A: [0, 255],
      I: [0, 255],
    },
    glio_yellow: {
      R: [0, 250],
      G: [0, 204],
      B: [0, 21],
      A: [0, 255],
      I: [0, 255],
    },
    glio_blue: {
      R: [0, 59],
      G: [0, 126],
      B: [0, 248],
      A: [0, 255],
      I: [0, 255],
    },
    glio_green: {
      R: [0, 34],
      G: [0, 197],
      B: [0, 94],
      A: [0, 255],
      I: [0, 255],
    },
    apt_heat: {
      R: [0, 23, 48, 252, 239],
      G: [0, 45, 178, 211, 68],
      B: [0, 140, 229, 77, 68],
      A: [0, 255, 255, 255, 255],
      I: [0, 64, 128, 192, 255],
    },
  };

  const LABEL_COLORMAP = {
    et:              'glio_red',   enhancing_tumor: 'glio_red',
    netc:            'glio_blue',  necrosis:        'glio_blue',
    necrotic_core:   'glio_blue',
    snfh:            'glio_yellow', edema:          'glio_yellow',
    whole_tumor:     'green', wt:              'green',
    tumor_core:      'glio_blue',  tc:         'glio_blue',
    tumor_mask:      'glio_red',
    rc:              'glio_green', resection_cavity:'glio_green',
    brain_mask:      'green',
  };

  function colormapFor(label) {
    return LABEL_COLORMAP[(label || '').toLowerCase()] || 'warm';
  }

  function fileUrl(relPath) {
    const encoded = relPath.split('/').map(encodeURIComponent).join('/');
    return `/api/files/${encoded}`;
  }

  function _clamp01(value) {
    if (!Number.isFinite(value)) return 0;
    return Math.min(1, Math.max(0, value));
  }

  function _overlaySignature(overlays = []) {
    return overlays.map(item => `${item.url || ''}|${item.label || ''}|${item.name || ''}`).join('::');
  }

  function _seriesSignature(seriesPaths = []) {
    return seriesPaths
      .map((item) => _normalizeSeriesItem(item))
      .filter((item) => item.path)
      .map((item) => `${item.path}|${item.colormap}`)
      .join('::');
  }

  function _normalizeSeriesItem(item) {
    if (!item) return { path: '', colormap: 'gray', name: '' };
    if (typeof item === 'string') {
      return {
        path: item,
        colormap: 'gray',
        name: item.split('/').pop(),
      };
    }
    const path = item.path || item.seqPath || '';
    return {
      path,
      colormap: 'gray',
      name: item.name || (path ? path.split('/').pop() : ''),
    };
  }

  function _touchScene(nv) {
    if (!nv) return;
    if (typeof nv.updateGLVolume === 'function') nv.updateGLVolume();
    if (typeof nv.drawScene === 'function') nv.drawScene();
    if (typeof nv.drawSceneCore === 'function') nv.drawSceneCore();
  }

  function _setVolumeOpacity(volume, opacity) {
    if (!volume) return;
    volume.opacity = opacity;
    if (typeof volume.setOpacity === 'function') volume.setOpacity(opacity);
  }

  function _applyBlend(nv, blendFactor = 0) {
    if (!nv?.volumes?.length) return;
    const meta = nv.__glioBlendMeta || {};
    const factor = _clamp01(blendFactor);
    const sourcePath = meta.sourcePath || null;
    const targetPath = meta.targetPath || null;
    const indexes = meta.seriesIndexByPath || {};

    for (const volumeIndex of Object.values(indexes)) {
      if (Number.isInteger(volumeIndex) && nv.volumes[volumeIndex]) {
        _setVolumeOpacity(nv.volumes[volumeIndex], 0);
      }
    }

    const sourceIndex = indexes[sourcePath];
    const targetIndex = indexes[targetPath];
    const hasTarget = !!targetPath && Number.isInteger(targetIndex);

    if (Number.isInteger(sourceIndex) && nv.volumes[sourceIndex]) {
      _setVolumeOpacity(nv.volumes[sourceIndex], hasTarget ? 1 - factor : 1);
    }
    if (hasTarget && nv.volumes[targetIndex]) {
      _setVolumeOpacity(nv.volumes[targetIndex], factor);
    }
    _touchScene(nv);
  }

  function _getNiivueCls() {
    if (typeof Niivue !== 'undefined') return Niivue;
    if (typeof niivue !== 'undefined' && niivue.Niivue) return niivue.Niivue;
    return null;
  }

  /**
   * Create a NiiVue instance on canvasEl, showing a single slice type.
   * sliceType: 0=axial  1=coronal  2=sagittal  3=render(3D)
   */
  async function createInstance(canvasEl, sliceType = 0) {
    const Cls = _getNiivueCls();
    if (!Cls) { console.warn('[GlioViewer] NiiVue CDN not loaded'); return null; }

    const nv = new Cls({
      backColor:       [0, 0, 0, 1],
      crosshairColor:  [0.23, 0.50, 1.0, 0.85],
      crosshairWidth:  1,
      show3Dcrosshair: false,
      isColorbar:      false,
      isOrientCube:    false,
      dragMode:        1,   // 1=contrast for 2D; in render mode NiiVue rotates by default
      logging:         false,
    });
    await nv.attachToCanvas(canvasEl);
    Object.entries(CUSTOM_COLORMAPS).forEach(([key, cmap]) => nv.addColormap(key, cmap));
    nv.setSliceType(sliceType);
    return nv;
  }

  /**
   * Load a sequence + overlays into a NiiVue instance.
   * Subsequent calls replace the previously loaded volume.
   */
  async function loadInto(nv, seqPath, overlays = []) {
    if (!nv) return;
    const base = _normalizeSeriesItem(seqPath);
    const volumes = [];
    if (base.path) {
      volumes.push({
        url:      fileUrl(base.path),
        name:     base.name,
        colormap: base.colormap,
        opacity:  1.0,
      });
    }
    for (const { url, label, name } of overlays) {
      if (!url) continue;
      volumes.push({
        url,
        name:     name || url.split('/').pop().split('?')[0],
        colormap: colormapFor(label),
        cal_min:  0,
        cal_max:  1,
        opacity:  0.68,
      });
    }
    if (volumes.length === 0) return;
    await nv.loadVolumes(volumes);
  }

  async function preloadBlendSet(nv, seriesPaths = [], overlays = []) {
    if (!nv) return;
    const uniqueItems = [];
    const seenPaths = new Set();
    for (const item of (seriesPaths || []).map((entry) => _normalizeSeriesItem(entry))) {
      if (!item.path || seenPaths.has(item.path)) continue;
      seenPaths.add(item.path);
      uniqueItems.push(item);
    }
    if (!uniqueItems.length) return;

    const seriesSig = _seriesSignature(uniqueItems);
    const overlaySig = _overlaySignature(overlays);
    const currentMeta = nv.__glioBlendMeta || null;
    const canReuse =
      currentMeta &&
      currentMeta.seriesSig === seriesSig &&
      currentMeta.overlaySig === overlaySig &&
      currentMeta.cached === true;

    if (canReuse) return;

    const volumes = [];
    const seriesIndexByPath = {};

    uniqueItems.forEach((item) => {
      seriesIndexByPath[item.path] = volumes.length;
      volumes.push({
        url: fileUrl(item.path),
        name: item.name,
        colormap: item.colormap,
        opacity: 0,
      });
    });

    for (const { url, label, name } of overlays) {
      if (!url) continue;
      volumes.push({
        url,
        name:     name || url.split('/').pop().split('?')[0],
        colormap: colormapFor(label),
        cal_min:  0,
        cal_max:  1,
        opacity:  0.68,
      });
    }

    await nv.loadVolumes(volumes);
    nv.__glioBlendMeta = {
      cached: true,
      sourcePath: uniqueItems[0].path,
      targetPath: null,
      overlaySig,
      seriesSig,
      seriesPaths: uniqueItems.map((item) => item.path),
      seriesIndexByPath,
    };
  }

  function setBlendState(nv, sourcePath, targetPath = null, blendFactor = 0) {
    if (!nv) return false;
    const meta = nv.__glioBlendMeta || null;
    if (!meta?.cached || !meta.seriesIndexByPath) return false;
    if (!sourcePath || !Number.isInteger(meta.seriesIndexByPath[sourcePath])) return false;

    const normalizedTarget = targetPath && targetPath !== sourcePath ? targetPath : null;
    if (normalizedTarget && !Number.isInteger(meta.seriesIndexByPath[normalizedTarget])) return false;

    meta.sourcePath = sourcePath;
    meta.targetPath = normalizedTarget;
    _applyBlend(nv, blendFactor);
    return true;
  }

  async function loadBlendInto(nv, sourcePath, targetPath = null, blendFactor = 0, overlays = []) {
    const source = _normalizeSeriesItem(sourcePath);
    const target = _normalizeSeriesItem(targetPath);
    if (!nv || !source.path) return;
    const normalizedTarget = target.path && target.path !== source.path ? target.path : null;
    const overlaySig = _overlaySignature(overlays);
    const currentMeta = nv.__glioBlendMeta || null;
    const canReuseCached =
      currentMeta?.cached &&
      currentMeta.overlaySig === overlaySig &&
      currentMeta.seriesIndexByPath &&
      Number.isInteger(currentMeta.seriesIndexByPath[source.path]) &&
      (!normalizedTarget || Number.isInteger(currentMeta.seriesIndexByPath[normalizedTarget]));

    if (canReuseCached) {
      setBlendState(nv, source.path, normalizedTarget, blendFactor);
      return;
    }

    const canReuse =
      currentMeta &&
      !currentMeta.cached &&
      currentMeta.sourcePath === source.path &&
      currentMeta.targetPath === normalizedTarget &&
      currentMeta.overlaySig === overlaySig;

    if (canReuse) {
      _applyBlend(nv, blendFactor);
      return;
    }

    const volumes = [
      {
        url: fileUrl(source.path),
        name: source.name,
        colormap: source.colormap,
        opacity: normalizedTarget ? 1 - _clamp01(blendFactor) : 1.0,
      },
    ];

    if (normalizedTarget) {
      volumes.push({
        url: fileUrl(normalizedTarget),
        name: target.name || normalizedTarget.split('/').pop(),
        colormap: target.colormap || 'gray',
        opacity: _clamp01(blendFactor),
      });
    }

    for (const { url, label, name } of overlays) {
      if (!url) continue;
      volumes.push({
        url,
        name:     name || url.split('/').pop().split('?')[0],
        colormap: colormapFor(label),
        cal_min:  0,
        cal_max:  1,
        opacity:  0.68,
      });
    }

    await nv.loadVolumes(volumes);
    nv.__glioBlendMeta = {
      cached: false,
      sourcePath: source.path,
      targetPath: normalizedTarget,
      overlaySig,
      seriesSig: _seriesSignature([source, normalizedTarget ? target : null].filter(Boolean)),
      seriesPaths: [source.path, normalizedTarget].filter(Boolean),
      seriesIndexByPath: normalizedTarget
        ? { [source.path]: 0, [normalizedTarget]: 1 }
        : { [source.path]: 0 },
    };
    _applyBlend(nv, blendFactor);
  }

  /**
   * Link all instances bidirectionally so crosshair / slice position
   * is synchronised across all panels.
   */
  function linkAll(nvList) {
    const valid = nvList.filter(Boolean);
    valid.forEach(nv => {
      nv.broadcastTo(valid.filter(other => other !== nv));
    });
  }

  // ── Zoom ─────────────────────────────────────────────────────
  // NiiVue 0.68+ stores 2D zoom in scene.pan2Dxyzmm[3] (not scene.zoom)
  function _getZoom(nv) {
    return nv?.scene?.pan2Dxyzmm?.[3] ?? 1;
  }

  function _setZoom(nv, value) {
    if (!nv?.scene?.pan2Dxyzmm) return;
    nv.scene.pan2Dxyzmm[3] = value;
    nv.drawScene?.();
  }

  function attachZoom(nv, canvasEl, onZoomChange) {
    if (!nv || !canvasEl) return;
    // NiiVue already handles wheel zoom internally on the canvas.
    // We hook the same event only to sync our display label.
    canvasEl.addEventListener('wheel', () => {
      requestAnimationFrame(() => onZoomChange?.(_getZoom(nv)));
    }, { passive: true });
  }

  function resetZoom(nv, onZoomChange) {
    if (!nv?.scene?.pan2Dxyzmm) return;
    nv.scene.pan2Dxyzmm[0] = 0;
    nv.scene.pan2Dxyzmm[1] = 0;
    nv.scene.pan2Dxyzmm[2] = 0;
    nv.scene.pan2Dxyzmm[3] = 1;
    nv.drawScene?.();
    onZoomChange?.(1);
  }

  function zoomIn(nv, onZoomChange) {
    const next = Math.min(20, _getZoom(nv) * 1.2);
    _setZoom(nv, next);
    onZoomChange?.(next);
  }

  function zoomOut(nv, onZoomChange) {
    const next = Math.max(0.1, _getZoom(nv) / 1.2);
    _setZoom(nv, next);
    onZoomChange?.(next);
  }

  // ── Voxel readout ─────────────────────────────────────────────
  function attachVoxelReadout(nv, displayEl) {
    if (!nv || !displayEl) return;
    nv.onLocationChange = (data) => {
      if (!data) { displayEl.textContent = '—'; return; }
      const vals = data.values;
      if (Array.isArray(vals) && vals.length > 0) {
        const v = vals[0];
        const num = typeof v.value === 'number' ? v.value.toFixed(1) : String(v.value ?? '—');
        displayEl.textContent = num;
      } else if (data.string) {
        // Extract numeric value from NiiVue's formatted string (e.g. "v=123.4")
        const m = data.string.match(/v\s*=\s*([\-\d.]+)/);
        displayEl.textContent = m ? m[1] : data.string;
      } else {
        displayEl.textContent = '—';
      }
    };
  }

  // ── Window / Level ────────────────────────────────────────────
  function getVolumeRange(nv) {
    if (!nv?.volumes?.length) return null;
    const vol = nv.volumes[0];
    const dataMin = vol.global_min ?? vol.robust_min ?? 0;
    const dataMax = vol.global_max ?? vol.robust_max ?? 1000;
    const calMin  = vol.cal_min  ?? dataMin;
    const calMax  = vol.cal_max  ?? dataMax;
    return { dataMin, dataMax, calMin, calMax,
             width: calMax - calMin,
             level: (calMax + calMin) / 2 };
  }

  function applyWL(nv, width, level) {
    if (!nv?.volumes?.length) return;
    const vol = nv.volumes[0];
    vol.cal_min = level - width / 2;
    vol.cal_max = level + width / 2;
    nv.updateGLVolume?.();
    nv.drawScene?.();
  }

  return { createInstance, loadInto, preloadBlendSet, setBlendState, loadBlendInto, linkAll,
           colormapFor, fileUrl, attachZoom, resetZoom, zoomIn, zoomOut, attachVoxelReadout, getVolumeRange, applyWL };
})();

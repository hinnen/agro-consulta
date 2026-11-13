/* SistApp — só a câmera de trás, com nitidez pedida no mesmo pedido.
   O Chrome não lista as lentes de trás separadas. Não há troca. */
(function () {
  try {
    localStorage.removeItem("sistapp_camera_id_v1");
    localStorage.removeItem("sistapp_camera_id_v2");
  } catch (e) {}

  function restricoes() {
    return {
      facingMode: { ideal: "environment" },
      width: { ideal: 1920 },
      height: { ideal: 1080 },
      advanced: [{ focusMode: "continuous" }],
    };
  }

  function aplicar(config) {
    var cfg = {};
    var k;
    if (config) {
      for (k in config) {
        if (Object.prototype.hasOwnProperty.call(config, k)) cfg[k] = config[k];
      }
    }
    cfg.videoConstraints = restricoes();
    if (!cfg.aspectRatio) cfg.aspectRatio = 1.7777778;
    return cfg;
  }

  function afiar(raiz) {
    var video = raiz && raiz.querySelector ? raiz.querySelector("video") : null;
    var stream = video && video.srcObject;
    var track = stream && stream.getVideoTracks && stream.getVideoTracks()[0];
    if (!track || typeof track.applyConstraints !== "function") return Promise.resolve();
    var caps = {};
    try {
      caps = typeof track.getCapabilities === "function" ? track.getCapabilities() : {};
    } catch (e) {
      caps = {};
    }
    var modos = caps && caps.focusMode;
    var tem = false;
    if (modos && typeof modos.indexOf === "function") tem = modos.indexOf("continuous") >= 0;
    if (!tem) return Promise.resolve();
    return track.applyConstraints({ advanced: [{ focusMode: "continuous" }] }).catch(function () {
      return track.applyConstraints({ focusMode: "continuous" }).catch(function () {});
    });
  }

  window.SistAppCamera = {
    restricoes: restricoes,
    aplicar: aplicar,
    afiar: afiar,
  };
})();

/* SistApp — escolhe a câmera de trás mais nítida e deixa o operador trocar. */
(function () {
  var LS = "sistapp_camera_id_v2";

  function labelDe(cam) {
    return String((cam && (cam.label || cam.id)) || "");
  }

  function nota(cam) {
    var s = labelDe(cam).toLowerCase();
    var ultra = /ultra|wide|0\.5|grande|angular|macro|depth/.test(s);
    var front = /front|user|frontal|face|selfie/.test(s);
    var back = /back|rear|traseir|environment|posterior/.test(s);
    if (front && !back) return 0;
    if (back && !ultra) return 30;
    if (ultra) return 10;
    if (s) return 20;
    return 15;
  }

  function ordenar(cams) {
    return (cams || []).slice().sort(function (a, b) {
      return nota(b) - nota(a);
    });
  }

  function getCameras() {
    if (!window.Html5Qrcode || typeof Html5Qrcode.getCameras !== "function") {
      return Promise.resolve([]);
    }
    return Html5Qrcode.getCameras().then(function (lista) {
      return Array.isArray(lista) ? lista : [];
    }).catch(function () {
      return [];
    });
  }

  function salvaId() {
    try {
      return localStorage.getItem(LS) || "";
    } catch (e) {
      return "";
    }
  }

  function lembrar(cameraId) {
    if (typeof cameraId !== "string" || !cameraId) return;
    try {
      localStorage.setItem(LS, cameraId);
    } catch (e) {}
  }

  function escolher(cams) {
    if (!cams || !cams.length) return { facingMode: "environment" };
    var id = salvaId();
    var i;
    if (id) {
      for (i = 0; i < cams.length; i++) {
        if (cams[i] && cams[i].id === id) return id;
      }
    }
    var ordem = ordenar(cams);
    return (ordem[0] && ordem[0].id) || { facingMode: "environment" };
  }

  function proxima(cams, atual) {
    if (!cams || cams.length < 2) return atual || { facingMode: "environment" };
    var ordem = ordenar(cams);
    var atualId = typeof atual === "string" ? atual : "";
    var idx = -1;
    var i;
    for (i = 0; i < ordem.length; i++) {
      if (ordem[i] && ordem[i].id === atualId) idx = i;
    }
    var n = ordem[(idx + 1) % ordem.length];
    if (n && n.id) {
      lembrar(n.id);
      return n.id;
    }
    return { facingMode: "environment" };
  }

  function alvo(cameraId) {
    if (typeof cameraId === "string" && cameraId) return cameraId;
    var salvo = salvaId();
    if (salvo) return salvo;
    return { facingMode: "environment" };
  }

  function idDoVideo(video) {
    try {
      var stream = video && video.srcObject;
      var track = stream && stream.getVideoTracks && stream.getVideoTracks()[0];
      var settings = track && track.getSettings && track.getSettings();
      return (settings && settings.deviceId) || "";
    } catch (e) {
      return "";
    }
  }

  window.SistAppCamera = {
    getCameras: getCameras,
    escolher: escolher,
    proxima: proxima,
    lembrar: lembrar,
    alvo: alvo,
    idDoVideo: idDoVideo,
  };
})();

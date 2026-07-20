/* Catálogo de sabores — Dispenser A6 (ícone + título + descrição curta) */
window.DspFlavorLib = (function () {
  function norm(s) {
    return String(s || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/-/g, " ")
      .replace(/\s+/g, " ");
  }

  var META = {
    frango: { ico: "ico-frango", img: "icons/frango.png", desc: "Proteína leve e de fácil digestão" },
    carne: { ico: "ico-carne", img: "icons/carne.png", desc: "Fonte de proteína de alta qualidade" },
    "carne bovina": { ico: "ico-carne", img: "icons/carne.png", desc: "Fonte de proteína de alta qualidade" },
    cordeiro: { ico: "ico-cordeiro", desc: "Proteína nobre e sabor marcante" },
    ovelha: { ico: "ico-cordeiro", desc: "Proteína nobre e sabor marcante" },
    salmao: { ico: "ico-salmao", desc: "Rico em ômega-3 e sabor suave" },
    atum: { ico: "ico-atum", desc: "Proteína magra de peixe" },
    peixe: { ico: "ico-peixe", desc: "Proteína leve e nutritiva" },
    "peixe branco": { ico: "ico-peixe", desc: "Proteína leve e nutritiva" },
    sardinha: { ico: "ico-sardinha", desc: "Fonte natural de ômega-3" },
    "carne suina": { ico: "ico-suina", desc: "Proteína suculenta e energética" },
    peru: { ico: "ico-peru", desc: "Proteína magra de aves" },
    pato: { ico: "ico-pato", desc: "Sabor intenso e rico" },
    coelho: { ico: "ico-coelho", desc: "Proteína delicada e magra" },
    figado: { ico: "ico-figado", desc: "Fonte natural de vitaminas" },
    "arroz integral": { ico: "ico-arroz", desc: "Carboidrato com fibras" },
    "arroz branco": { ico: "ico-arroz", desc: "Energia de fácil absorção" },
    arroz: { ico: "ico-arroz", desc: "Energia equilibrada" },
    "batata doce": { ico: "ico-batata", img: "icons/batata.png", desc: "Carboidrato de qualidade e energia equilibrada" },
    cenoura: { ico: "ico-cenoura", desc: "Beta-caroteno e fibras" },
    abobora: { ico: "ico-abobora", desc: "Fibras e vitaminas naturais" },
    ervilha: { ico: "ico-ervilha", desc: "Proteína vegetal e fibras" },
    maca: { ico: "ico-maca", desc: "Fibras e sabor natural" },
    blueberry: { ico: "ico-mirtilo", desc: "Antioxidantes naturais" },
    mirtilo: { ico: "ico-mirtilo", desc: "Antioxidantes naturais" },
    "blueberry mirtilo": { ico: "ico-mirtilo", desc: "Antioxidantes naturais" },
    brocolis: { ico: "ico-brocolis", desc: "Vitaminas e fibras verdes" },
    espinafre: { ico: "ico-espinafre", desc: "Folhas verdes nutritivas" },
    beterraba: { ico: "ico-beterraba", desc: "Fibras e corantes naturais" },
    "polpa de beterraba": { ico: "ico-beterraba", desc: "Fibras e corantes naturais" },
    mandioca: { ico: "ico-mandioca", desc: "Energia de raiz" },
    quinoa: { ico: "ico-quinoa", desc: "Grão completo e leve" },
    aveia: { ico: "ico-aveia", desc: "Fibras e energia sustentada" },
    legumes: { ico: "ico-legumes", desc: "Variedade de vegetais" },
    vegetais: { ico: "ico-legumes", desc: "Variedade de vegetais" },
    "erva doce": { ico: "ico-erva", desc: "Aroma suave e digestivo" },
    hortela: { ico: "ico-hortela", desc: "Frescor natural" }
  };

  function lookup(name) {
    var key = norm(name);
    var hit = META[key];
    if (hit) {
      return {
        title: String(name || "").trim(),
        desc: hit.desc,
        ico: hit.ico,
        img: hit.img || ""
      };
    }
    var first = key.split(" ")[0];
    hit = META[first];
    if (hit) {
      return {
        title: String(name || "").trim(),
        desc: hit.desc,
        ico: hit.ico,
        img: hit.img || ""
      };
    }
    return {
      title: String(name || "").trim(),
      desc: "Ingrediente selecionado",
      ico: "ico-generico",
      img: ""
    };
  }

  /** Combinações prontas → arquivo de foto + linhas de sabor */
  var COMBOS = [
    { id: "frango-carne", file: "ings/combo-frango-carne.jpg", label: "Frango e Carne", flavors: "Frango\nCarne" },
    { id: "frango-arroz", file: "ings/combo-frango-arroz.jpg", label: "Frango e Arroz", flavors: "Frango\nArroz" },
    { id: "ovelha-arroz", file: "ings/combo-ovelha-arroz.jpg", label: "Ovelha e Arroz", flavors: "Ovelha\nArroz" },
    { id: "carne-vegetais", file: "ings/combo-carne-vegetais.jpg", label: "Carne e Vegetais", flavors: "Carne\nVegetais" },
    { id: "salmao-batata", file: "ings/combo-salmao-batata.jpg", label: "Salmão e Batata-Doce", flavors: "Salmão\nBatata doce" },
    { id: "frango-batata", file: "ings/combo-frango-batata.jpg", label: "Frango e Batata-Doce", flavors: "Frango\nBatata doce" },
    { id: "suina-quinoa", file: "ings/combo-suina-quinoa.jpg", label: "Carne Suína e Quinoa", flavors: "Carne suína\nQuinoa" },
    { id: "frango-maca", file: "ings/combo-frango-maca.jpg", label: "Frango e Maçã", flavors: "Frango\nMaçã" },
    { id: "salmao-arroz", file: "ings/combo-salmao-arroz.jpg", label: "Salmão e Arroz", flavors: "Salmão\nArroz" },
    {
      id: "frango-batata-brocolis-maca",
      file: "ings/combo-frango-batata-brocolis-maca.jpg",
      label: "Frango, Batata, Brócolis e Maçã",
      flavors: "Frango\nBatata doce\nBrócolis\nMaçã"
    },
    {
      id: "suina-quinoa-erva-hortela",
      file: "ings/combo-suina-quinoa-erva-hortela.jpg",
      label: "Suína, Quinoa, Erva-Doce e Hortelã",
      flavors: "Carne suína\nQuinoa\nErva doce\nHortelã"
    },
    { id: "peru-vegetais", file: "ings/combo-peru-vegetais.jpg", label: "Peru e Vegetais", flavors: "Peru\nVegetais" },
    /* legados */
    { id: "carne-frango-batata", file: "ings/carne-frango-batata.jpg", label: "Carne, Frango e Batata", flavors: "Carne\nFrango\nBatata doce" },
    { id: "peixe", file: "ings/peixe.jpg", label: "Peixe e Salmão", flavors: "Peixe\nSalmão" },
    { id: "carne-abobora", file: "ings/carne-abobora.jpg", label: "Carne e Abóbora", flavors: "Carne\nAbóbora" },
    { id: "frango-cenoura", file: "ings/frango-cenoura.jpg", label: "Frango, Cenoura e Arroz", flavors: "Frango\nCenoura\nArroz" },
    { id: "carne-legumes", file: "ings/carne-legumes.jpg", label: "Carne, Batata e Legumes", flavors: "Carne\nBatata doce\nLegumes" }
  ];

  return { norm: norm, lookup: lookup, META: META, COMBOS: COMBOS };
})();

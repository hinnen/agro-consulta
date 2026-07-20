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
    cordeiro: { ico: "ico-cordeiro", img: "icons/cordeiro.png", desc: "Proteína nobre e sabor marcante" },
    ovelha: { ico: "ico-cordeiro", img: "icons/cordeiro.png", desc: "Proteína nobre e sabor marcante" },
    salmao: { ico: "ico-salmao", img: "icons/salmao.png", desc: "Rico em ômega-3 e sabor suave" },
    atum: { ico: "ico-atum", img: "icons/atum.png", desc: "Proteína magra de peixe" },
    peixe: { ico: "ico-peixe", img: "icons/peixe.png", desc: "Proteína leve e nutritiva" },
    "peixe branco": { ico: "ico-peixe", img: "icons/peixe.png", desc: "Proteína leve e nutritiva" },
    sardinha: { ico: "ico-sardinha", img: "icons/sardinha.png", desc: "Fonte natural de ômega-3" },
    "carne suina": { ico: "ico-suina", img: "icons/suina.png", desc: "Proteína suculenta e energética" },
    peru: { ico: "ico-peru", img: "icons/peru.png", desc: "Proteína magra de aves" },
    pato: { ico: "ico-pato", img: "icons/pato.png", desc: "Sabor intenso e rico" },
    coelho: { ico: "ico-coelho", img: "icons/coelho.png", desc: "Proteína delicada e magra" },
    figado: { ico: "ico-figado", img: "icons/figado.png", desc: "Fonte natural de vitaminas" },
    "arroz integral": { ico: "ico-arroz", img: "icons/arroz.png", desc: "Carboidrato com fibras" },
    "arroz branco": { ico: "ico-arroz", img: "icons/arroz.png", desc: "Energia de fácil absorção" },
    arroz: { ico: "ico-arroz", img: "icons/arroz.png", desc: "Energia equilibrada" },
    "batata doce": { ico: "ico-batata", img: "icons/batata.png", desc: "Carboidrato de qualidade e energia equilibrada" },
    cenoura: { ico: "ico-cenoura", img: "icons/cenoura.png", desc: "Beta-caroteno e fibras" },
    abobora: { ico: "ico-abobora", img: "icons/abobora.png", desc: "Fibras e vitaminas naturais" },
    ervilha: { ico: "ico-ervilha", img: "icons/ervilha.png", desc: "Proteína vegetal e fibras" },
    maca: { ico: "ico-maca", img: "icons/maca.png", desc: "Fibras e sabor natural" },
    blueberry: { ico: "ico-mirtilo", img: "icons/mirtilo.png", desc: "Antioxidantes naturais" },
    mirtilo: { ico: "ico-mirtilo", img: "icons/mirtilo.png", desc: "Antioxidantes naturais" },
    "blueberry mirtilo": { ico: "ico-mirtilo", img: "icons/mirtilo.png", desc: "Antioxidantes naturais" },
    brocolis: { ico: "ico-brocolis", img: "icons/brocolis.png", desc: "Vitaminas e fibras verdes" },
    espinafre: { ico: "ico-espinafre", img: "icons/espinafre.png", desc: "Folhas verdes nutritivas" },
    beterraba: { ico: "ico-beterraba", img: "icons/beterraba.png", desc: "Fibras e corantes naturais" },
    "polpa de beterraba": { ico: "ico-beterraba", img: "icons/beterraba.png", desc: "Fibras e corantes naturais" },
    mandioca: { ico: "ico-mandioca", img: "icons/mandioca.png", desc: "Energia de raiz" },
    quinoa: { ico: "ico-quinoa", img: "icons/quinoa.png", desc: "Grão completo e leve" },
    aveia: { ico: "ico-aveia", img: "icons/aveia.png", desc: "Fibras e energia sustentada" },
    legumes: { ico: "ico-legumes", img: "icons/legumes.png", desc: "Variedade de vegetais" },
    vegetais: { ico: "ico-legumes", img: "icons/legumes.png", desc: "Variedade de vegetais" },
    "erva doce": { ico: "ico-erva", img: "icons/erva.png", desc: "Aroma suave e digestivo" },
    hortela: { ico: "ico-hortela", img: "icons/hortela.png", desc: "Frescor natural" }
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

  /** Fotos individuais (biblioteca de ingredientes) */
  var SINGLES = [
    { id: "solo-frango", file: "ings/solo-frango.jpg", label: "Frango", flavors: "Frango" },
    { id: "solo-carne", file: "ings/solo-carne.jpg", label: "Carne Bovina", flavors: "Carne Bovina" },
    { id: "solo-cordeiro", file: "ings/solo-cordeiro.jpg", label: "Cordeiro", flavors: "Cordeiro" },
    { id: "solo-salmao", file: "ings/solo-salmao.jpg", label: "Salmão", flavors: "Salmão" },
    { id: "solo-atum", file: "ings/solo-atum.jpg", label: "Atum", flavors: "Atum" },
    { id: "solo-peixe", file: "ings/solo-peixe.jpg", label: "Peixe Branco", flavors: "Peixe Branco" },
    { id: "solo-sardinha", file: "ings/solo-sardinha.jpg", label: "Sardinha", flavors: "Sardinha" },
    { id: "solo-suina", file: "ings/solo-suina.jpg", label: "Carne Suína", flavors: "Carne Suína" },
    { id: "solo-peru", file: "ings/solo-peru.jpg", label: "Peru", flavors: "Peru" },
    { id: "solo-pato", file: "ings/solo-pato.jpg", label: "Pato", flavors: "Pato" },
    { id: "solo-coelho", file: "ings/solo-coelho.jpg", label: "Coelho", flavors: "Coelho" },
    { id: "solo-figado", file: "ings/solo-figado.jpg", label: "Fígado", flavors: "Fígado" },
    { id: "solo-ovelha", file: "ings/solo-ovelha.jpg", label: "Ovelha", flavors: "Ovelha" },
    { id: "solo-arroz", file: "ings/solo-arroz.jpg", label: "Arroz", flavors: "Arroz" },
    { id: "solo-batata", file: "ings/solo-batata.jpg", label: "Batata-Doce", flavors: "Batata-Doce" },
    { id: "solo-cenoura", file: "ings/solo-cenoura.jpg", label: "Cenoura", flavors: "Cenoura" },
    { id: "solo-abobora", file: "ings/solo-abobora.jpg", label: "Abóbora", flavors: "Abóbora" },
    { id: "solo-ervilha", file: "ings/solo-ervilha.jpg", label: "Ervilha", flavors: "Ervilha" },
    { id: "solo-maca", file: "ings/solo-maca.jpg", label: "Maçã", flavors: "Maçã" },
    { id: "solo-mirtilo", file: "ings/solo-mirtilo.jpg", label: "Blueberry (Mirtilo)", flavors: "Blueberry (Mirtilo)" },
    { id: "solo-brocolis", file: "ings/solo-brocolis.jpg", label: "Brócolis", flavors: "Brócolis" },
    { id: "solo-espinafre", file: "ings/solo-espinafre.jpg", label: "Espinafre", flavors: "Espinafre" },
    { id: "solo-beterraba", file: "ings/solo-beterraba.jpg", label: "Beterraba", flavors: "Beterraba" },
    { id: "solo-mandioca", file: "ings/solo-mandioca.jpg", label: "Mandioca", flavors: "Mandioca" },
    { id: "solo-quinoa", file: "ings/solo-quinoa.jpg", label: "Quinoa", flavors: "Quinoa" },
    { id: "solo-aveia", file: "ings/solo-aveia.jpg", label: "Aveia", flavors: "Aveia" },
    { id: "solo-erva", file: "ings/solo-erva.jpg", label: "Erva-Doce", flavors: "Erva-Doce" },
    { id: "solo-hortela", file: "ings/solo-hortela.jpg", label: "Hortelã", flavors: "Hortelã" },
    { id: "solo-legumes", file: "ings/solo-legumes.jpg", label: "Legumes", flavors: "Legumes" }
  ];

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

  /** Lista canônica — proteínas e bases */
  var PROTEINS = [
    { key: "frango", label: "Frango" },
    { key: "carne bovina", label: "Carne Bovina" },
    { key: "cordeiro", label: "Cordeiro" },
    { key: "salmao", label: "Salmão" },
    { key: "atum", label: "Atum" },
    { key: "peixe branco", label: "Peixe Branco" },
    { key: "sardinha", label: "Sardinha" },
    { key: "carne suina", label: "Carne Suína" },
    { key: "peru", label: "Peru" },
    { key: "pato", label: "Pato" },
    { key: "coelho", label: "Coelho" },
    { key: "figado", label: "Fígado" },
    { key: "ovelha", label: "Ovelha" }
  ];

  /** Lista canônica — acompanhamentos */
  var SIDES = [
    { key: "arroz integral", label: "Arroz Integral" },
    { key: "arroz branco", label: "Arroz Branco" },
    { key: "batata doce", label: "Batata-Doce" },
    { key: "cenoura", label: "Cenoura" },
    { key: "abobora", label: "Abóbora" },
    { key: "ervilha", label: "Ervilha" },
    { key: "maca", label: "Maçã" },
    { key: "mirtilo", label: "Blueberry (Mirtilo)" },
    { key: "brocolis", label: "Brócolis" },
    { key: "espinafre", label: "Espinafre" },
    { key: "beterraba", label: "Beterraba" },
    { key: "polpa de beterraba", label: "Polpa de Beterraba" },
    { key: "mandioca", label: "Mandioca" },
    { key: "quinoa", label: "Quinoa" },
    { key: "aveia", label: "Aveia" },
    { key: "erva doce", label: "Erva-Doce" },
    { key: "hortela", label: "Hortelã" },
    { key: "legumes", label: "Legumes" },
    { key: "vegetais", label: "Vegetais" }
  ];

  var INDIVIDUALS = PROTEINS.concat(SIDES);

  function individuals() {
    return INDIVIDUALS.slice();
  }

  return {
    norm: norm,
    lookup: lookup,
    META: META,
    SINGLES: SINGLES,
    COMBOS: SINGLES.concat(COMBOS),
    PROTEINS: PROTEINS,
    SIDES: SIDES,
    INDIVIDUALS: INDIVIDUALS,
    individuals: individuals
  };
})();

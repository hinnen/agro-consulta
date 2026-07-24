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
    hortela: { ico: "ico-hortela", img: "icons/hortela.png", desc: "Frescor natural" },
    leite: { ico: "ico-leite", img: "icons/leite.png", desc: "Cálcio e sabor cremoso" },
    "leite integral": { ico: "ico-leite", img: "icons/leite.png", desc: "Cálcio e sabor cremoso" },
    ovo: { ico: "ico-ovo", img: "icons/ovo.png", desc: "Proteína completa e leve" },
    ovos: { ico: "ico-ovo", img: "icons/ovo.png", desc: "Proteína completa e leve" },
    bacalhau: { ico: "ico-bacalhau", img: "icons/bacalhau.png", desc: "Peixe branco de sabor suave" },
    veado: { ico: "ico-veado", img: "icons/veado.png", desc: "Proteína magra e diferenciada" },
    cervo: { ico: "ico-veado", img: "icons/veado.png", desc: "Proteína magra e diferenciada" },
    camarao: { ico: "ico-camarao", img: "icons/camarao.png", desc: "Sabor do mar, rico e marcante" },
    batata: { ico: "ico-batata-branca", img: "icons/batata-branca.png", desc: "Energia de fácil digestão" },
    "batata inglesa": { ico: "ico-batata-branca", img: "icons/batata-branca.png", desc: "Energia de fácil digestão" },
    milho: { ico: "ico-milho", img: "icons/milho.png", desc: "Energia e fibras naturais" },
    linhaca: { ico: "ico-linhaca", img: "icons/linhaca.png", desc: "Ômega-3 vegetal e fibras" },
    cranberry: { ico: "ico-cranberry", img: "icons/cranberry.png", desc: "Antioxidantes e suporte urinário" },
    oxicoco: { ico: "ico-cranberry", img: "icons/cranberry.png", desc: "Antioxidantes e suporte urinário" },
    banana: { ico: "ico-banana", img: "icons/banana.png", desc: "Potássio e energia natural" },
    couve: { ico: "ico-couve", img: "icons/couve.png", desc: "Folhas verdes e vitaminas" },
    inhame: { ico: "ico-inhame", img: "icons/inhame.png", desc: "Carboidrato de raiz suave" },
    alecrim: { ico: "ico-alecrim", img: "icons/alecrim.png", desc: "Aroma natural e antioxidante" }
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
    { id: "solo-frango", file: "ings/solo-frango.png", label: "Frango", flavors: "Frango" },
    { id: "solo-carne", file: "ings/solo-carne.png", label: "Carne Bovina", flavors: "Carne Bovina" },
    { id: "solo-cordeiro", file: "ings/solo-cordeiro.png", label: "Cordeiro", flavors: "Cordeiro" },
    { id: "solo-salmao", file: "ings/solo-salmao.png", label: "Salmão", flavors: "Salmão" },
    { id: "solo-atum", file: "ings/solo-atum.png", label: "Atum", flavors: "Atum" },
    { id: "solo-peixe", file: "ings/solo-peixe.png", label: "Peixe Branco", flavors: "Peixe Branco" },
    { id: "solo-sardinha", file: "ings/solo-sardinha.png", label: "Sardinha", flavors: "Sardinha" },
    { id: "solo-suina", file: "ings/solo-suina.png", label: "Carne Suína", flavors: "Carne Suína" },
    { id: "solo-peru", file: "ings/solo-peru.png", label: "Peru", flavors: "Peru" },
    { id: "solo-pato", file: "ings/solo-pato.png", label: "Pato", flavors: "Pato" },
    { id: "solo-coelho", file: "ings/solo-coelho.png", label: "Coelho", flavors: "Coelho" },
    { id: "solo-figado", file: "ings/solo-figado.png", label: "Fígado", flavors: "Fígado" },
    { id: "solo-ovelha", file: "ings/solo-ovelha.png", label: "Ovelha", flavors: "Ovelha" },
    { id: "solo-arroz", file: "ings/solo-arroz.png", label: "Arroz", flavors: "Arroz" },
    { id: "solo-batata", file: "ings/solo-batata.png", label: "Batata-Doce", flavors: "Batata-Doce" },
    { id: "solo-cenoura", file: "ings/solo-cenoura.png", label: "Cenoura", flavors: "Cenoura" },
    { id: "solo-abobora", file: "ings/solo-abobora.png", label: "Abóbora", flavors: "Abóbora" },
    { id: "solo-ervilha", file: "ings/solo-ervilha.png", label: "Ervilha", flavors: "Ervilha" },
    { id: "solo-maca", file: "ings/solo-maca.png", label: "Maçã", flavors: "Maçã" },
    { id: "solo-mirtilo", file: "ings/solo-mirtilo.png", label: "Blueberry (Mirtilo)", flavors: "Blueberry (Mirtilo)" },
    { id: "solo-brocolis", file: "ings/solo-brocolis.png", label: "Brócolis", flavors: "Brócolis" },
    { id: "solo-espinafre", file: "ings/solo-espinafre.png", label: "Espinafre", flavors: "Espinafre" },
    { id: "solo-beterraba", file: "ings/solo-beterraba.png", label: "Beterraba", flavors: "Beterraba" },
    { id: "solo-mandioca", file: "ings/solo-mandioca.png", label: "Mandioca", flavors: "Mandioca" },
    { id: "solo-quinoa", file: "ings/solo-quinoa.png", label: "Quinoa", flavors: "Quinoa" },
    { id: "solo-aveia", file: "ings/solo-aveia.png", label: "Aveia", flavors: "Aveia" },
    { id: "solo-erva", file: "ings/solo-erva.png", label: "Erva-Doce", flavors: "Erva-Doce" },
    { id: "solo-hortela", file: "ings/solo-hortela.png", label: "Hortelã", flavors: "Hortelã" },
    { id: "solo-legumes", file: "ings/solo-legumes.png", label: "Legumes", flavors: "Legumes" }
  ];

  /** Combinações prontas → arquivo de foto + linhas de sabor */
  var COMBOS = [
    { id: "frango-carne", file: "ings/combo-frango-carne.png", label: "Frango e Carne", flavors: "Frango\nCarne" },
    { id: "frango-arroz", file: "ings/combo-frango-arroz.png", label: "Frango e Arroz", flavors: "Frango\nArroz" },
    { id: "ovelha-arroz", file: "ings/combo-ovelha-arroz.png", label: "Ovelha e Arroz", flavors: "Ovelha\nArroz" },
    { id: "carne-vegetais", file: "ings/combo-carne-vegetais.png", label: "Carne e Vegetais", flavors: "Carne\nVegetais" },
    { id: "salmao-batata", file: "ings/combo-salmao-batata.png", label: "Salmão e Batata-Doce", flavors: "Salmão\nBatata doce" },
    { id: "frango-batata", file: "ings/combo-frango-batata.png", label: "Frango e Batata-Doce", flavors: "Frango\nBatata doce" },
    { id: "suina-quinoa", file: "ings/combo-suina-quinoa.png", label: "Carne Suína e Quinoa", flavors: "Carne suína\nQuinoa" },
    { id: "frango-maca", file: "ings/combo-frango-maca.png", label: "Frango e Maçã", flavors: "Frango\nMaçã" },
    { id: "salmao-arroz", file: "ings/combo-salmao-arroz.png", label: "Salmão e Arroz", flavors: "Salmão\nArroz" },
    {
      id: "frango-batata-brocolis-maca",
      file: "ings/combo-frango-batata-brocolis-maca.png",
      label: "Frango, Batata, Brócolis e Maçã",
      flavors: "Frango\nBatata doce\nBrócolis\nMaçã"
    },
    {
      id: "suina-quinoa-erva-hortela",
      file: "ings/combo-suina-quinoa-erva-hortela.png",
      label: "Suína, Quinoa, Erva-Doce e Hortelã",
      flavors: "Carne suína\nQuinoa\nErva doce\nHortelã"
    },
    { id: "peru-vegetais", file: "ings/combo-peru-vegetais.png", label: "Peru e Vegetais", flavors: "Peru\nVegetais" },
    /* legados */
    { id: "carne-frango-batata", file: "ings/carne-frango-batata.png", label: "Carne, Frango e Batata", flavors: "Carne\nFrango\nBatata doce" },
    { id: "peixe", file: "ings/peixe.png", label: "Peixe e Salmão", flavors: "Peixe\nSalmão" },
    { id: "carne-abobora", file: "ings/carne-abobora.png", label: "Carne e Abóbora", flavors: "Carne\nAbóbora" },
    { id: "frango-cenoura", file: "ings/frango-cenoura.png", label: "Frango, Cenoura e Arroz", flavors: "Frango\nCenoura\nArroz" },
    { id: "carne-legumes", file: "ings/carne-legumes.png", label: "Carne, Batata e Legumes", flavors: "Carne\nBatata doce\nLegumes" }
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
    { key: "bacalhau", label: "Bacalhau" },
    { key: "carne suina", label: "Carne Suína" },
    { key: "peru", label: "Peru" },
    { key: "pato", label: "Pato" },
    { key: "coelho", label: "Coelho" },
    { key: "veado", label: "Veado" },
    { key: "figado", label: "Fígado" },
    { key: "ovelha", label: "Ovelha" },
    { key: "camarao", label: "Camarão" },
    { key: "ovo", label: "Ovo" },
    { key: "leite", label: "Leite" }
  ];

  /** Lista canônica — acompanhamentos */
  var SIDES = [
    { key: "arroz integral", label: "Arroz Integral" },
    { key: "arroz branco", label: "Arroz Branco" },
    { key: "batata doce", label: "Batata-Doce" },
    { key: "batata", label: "Batata" },
    { key: "cenoura", label: "Cenoura" },
    { key: "abobora", label: "Abóbora" },
    { key: "ervilha", label: "Ervilha" },
    { key: "maca", label: "Maçã" },
    { key: "mirtilo", label: "Blueberry (Mirtilo)" },
    { key: "cranberry", label: "Cranberry (Oxicoco)" },
    { key: "banana", label: "Banana" },
    { key: "brocolis", label: "Brócolis" },
    { key: "espinafre", label: "Espinafre" },
    { key: "couve", label: "Couve" },
    { key: "beterraba", label: "Beterraba" },
    { key: "polpa de beterraba", label: "Polpa de Beterraba" },
    { key: "mandioca", label: "Mandioca" },
    { key: "inhame", label: "Inhame" },
    { key: "quinoa", label: "Quinoa" },
    { key: "aveia", label: "Aveia" },
    { key: "milho", label: "Milho" },
    { key: "linhaca", label: "Linhaça" },
    { key: "erva doce", label: "Erva-Doce" },
    { key: "hortela", label: "Hortelã" },
    { key: "alecrim", label: "Alecrim" },
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

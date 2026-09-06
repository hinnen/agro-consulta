"""
Assinatura XML para SEFAZ (NFe/NFC-e, distDFe).

A norma fiscal brasileira exige RSA-SHA1 + digest SHA-1. O signxml >= 4 bloqueia SHA-1
por padrão; esta subclasse permite apenas esse uso legado obrigatório.
"""

from __future__ import annotations

from signxml import XMLSigner, methods
from signxml.algorithms import CanonicalizationMethod, DigestAlgorithm, SignatureMethod
from signxml.util import namespaces as signxml_ns

C14N_SEFAZ = CanonicalizationMethod.CANONICAL_XML_1_0


class SefazXMLSigner(XMLSigner):
    def check_deprecated_methods(self):
        pass


def criar_sefaz_xml_signer() -> SefazXMLSigner:
    signer = SefazXMLSigner(
        method=methods.enveloped,
        signature_algorithm=SignatureMethod.RSA_SHA1,
        digest_algorithm=DigestAlgorithm.SHA1,
        c14n_algorithm=C14N_SEFAZ,
    )
    # Signature sem prefixo ds: (exigência SEFAZ — rejeição 404)
    signer.namespaces = {None: signxml_ns.ds}
    return signer

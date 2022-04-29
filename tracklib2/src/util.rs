use chacha20poly1305::aead::rand_core::{OsRng, RngCore};
use chacha20poly1305::aead::{Aead, NewAead};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};

pub(crate) fn random_key_material() -> Vec<u8> {
    let key = XChaCha20Poly1305::generate_key(OsRng);
    key.to_vec()
}

pub(crate) fn encrypt(key_material: &[u8], buf: &[u8]) -> Result<Vec<u8>, String> {
    let key = Key::from_slice(key_material);
    let aead = XChaCha20Poly1305::new(key);

    let mut nonce_material = vec![0; orion::hazardous::stream::xchacha20::XCHACHA_NONCESIZE];
    OsRng.fill_bytes(&mut nonce_material);
    let nonce = XNonce::from_slice(&nonce_material);

    let mut out = vec![];
    let ciphertext = aead.encrypt(nonce, buf).map_err(|e| format!("{e:?}"))?;
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);

    Ok(out)
}

pub(crate) fn decrypt(key_material: &[u8], buf: &[u8]) -> Result<Vec<u8>, String> {
    let key = Key::from_slice(key_material);
    let aead = XChaCha20Poly1305::new(key);
    let nonce = XNonce::from_slice(&buf[..orion::hazardous::stream::xchacha20::XCHACHA_NONCESIZE]);
    aead.decrypt(
        nonce,
        &buf[orion::hazardous::stream::xchacha20::XCHACHA_NONCESIZE..],
    )
    .map_err(|e| format!("{e:?}"))
}

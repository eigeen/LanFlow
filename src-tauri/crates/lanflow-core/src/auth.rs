use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use hmac::{Hmac, Mac};
use opaque_ke::argon2::Argon2;
use opaque_ke::ciphersuite::CipherSuite;
use opaque_ke::{
    ClientLogin, ClientLoginFinishParameters, ClientRegistration,
    ClientRegistrationFinishParameters, CredentialFinalization, CredentialRequest,
    CredentialResponse, RegistrationRequest, RegistrationResponse, RegistrationUpload, ServerLogin,
    ServerLoginParameters, ServerRegistration, ServerSetup,
};
use opaque_rand::rngs::OsRng;
use rand::Rng;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::error::{LanFlowError, Result};

pub struct LanFlowCipherSuite;

impl CipherSuite for LanFlowCipherSuite {
    type OprfCs = opaque_ke::Ristretto255;
    type KeyExchange = opaque_ke::TripleDh<opaque_ke::Ristretto255, sha2::Sha512>;
    type Ksf = Argon2<'static>;
}

pub type LanFlowServerSetup = ServerSetup<LanFlowCipherSuite>;

pub fn new_server_setup() -> LanFlowServerSetup {
    ServerSetup::new(&mut OsRng)
}

pub fn serialize_server_setup(setup: &LanFlowServerSetup) -> Vec<u8> {
    setup.serialize().to_vec()
}

pub fn deserialize_server_setup(bytes: &[u8]) -> Result<LanFlowServerSetup> {
    ServerSetup::deserialize(bytes).map_err(|error| LanFlowError::Auth(error.to_string()))
}

pub fn register_password(
    setup: &LanFlowServerSetup,
    share_id: &str,
    password: &str,
) -> Result<Vec<u8>> {
    let mut client_rng = OsRng;
    let start =
        ClientRegistration::<LanFlowCipherSuite>::start(&mut client_rng, password.as_bytes())
            .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    let server = ServerRegistration::<LanFlowCipherSuite>::start(
        setup,
        RegistrationRequest::deserialize(&start.message.serialize())
            .map_err(|error| LanFlowError::Auth(error.to_string()))?,
        share_id.as_bytes(),
    )
    .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    let finish = start
        .state
        .finish(
            &mut client_rng,
            password.as_bytes(),
            RegistrationResponse::deserialize(&server.message.serialize())
                .map_err(|error| LanFlowError::Auth(error.to_string()))?,
            ClientRegistrationFinishParameters::default(),
        )
        .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    let record = ServerRegistration::finish(
        RegistrationUpload::<LanFlowCipherSuite>::deserialize(&finish.message.serialize())
            .map_err(|error| LanFlowError::Auth(error.to_string()))?,
    );
    Ok(record.serialize().to_vec())
}

pub struct ServerPendingLogin {
    state: ServerLogin<LanFlowCipherSuite>,
}

pub fn begin_server_login(
    setup: &LanFlowServerSetup,
    share_id: &str,
    password_record: &[u8],
    credential_request: &[u8],
) -> Result<(Vec<u8>, ServerPendingLogin)> {
    let password_file = ServerRegistration::<LanFlowCipherSuite>::deserialize(password_record)
        .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    let request = CredentialRequest::deserialize(credential_request)
        .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    let start = ServerLogin::start(
        &mut OsRng,
        setup,
        Some(password_file),
        request,
        share_id.as_bytes(),
        ServerLoginParameters::default(),
    )
    .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    Ok((
        start.message.serialize().to_vec(),
        ServerPendingLogin { state: start.state },
    ))
}

impl ServerPendingLogin {
    pub fn finish(self, finalization: &[u8]) -> Result<Vec<u8>> {
        let finalization = CredentialFinalization::deserialize(finalization)
            .map_err(|error| LanFlowError::Auth(error.to_string()))?;
        let result = self
            .state
            .finish(finalization, ServerLoginParameters::default())
            .map_err(|_| LanFlowError::Auth("密码不正确".into()))?;
        Ok(result.session_key.to_vec())
    }
}

pub struct ClientPendingLogin {
    state: ClientLogin<LanFlowCipherSuite>,
    password: Zeroizing<String>,
}

pub fn begin_client_login(password: String) -> Result<(Vec<u8>, ClientPendingLogin)> {
    let password = Zeroizing::new(password);
    let result = ClientLogin::<LanFlowCipherSuite>::start(&mut OsRng, password.as_bytes())
        .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    Ok((
        result.message.serialize().to_vec(),
        ClientPendingLogin {
            state: result.state,
            password,
        },
    ))
}

impl ClientPendingLogin {
    pub fn finish(self, response: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let response = CredentialResponse::deserialize(response)
            .map_err(|error| LanFlowError::Auth(error.to_string()))?;
        let result = self
            .state
            .finish(
                &mut OsRng,
                self.password.as_bytes(),
                response,
                ClientLoginFinishParameters::default(),
            )
            .map_err(|_| LanFlowError::Auth("密码不正确".into()))?;
        Ok((
            result.message.serialize().to_vec(),
            result.session_key.to_vec(),
        ))
    }
}

type HmacSha256 = Hmac<Sha256>;

pub fn mac(key: &[u8], parts: &[&[u8]]) -> Result<Vec<u8>> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    for part in parts {
        mac.update(part);
    }
    Ok(mac.finalize().into_bytes().to_vec())
}

pub fn verify_mac(key: &[u8], parts: &[&[u8]], proof: &[u8]) -> Result<()> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|error| LanFlowError::Auth(error.to_string()))?;
    for part in parts {
        mac.update(part);
    }
    mac.verify_slice(proof)
        .map_err(|_| LanFlowError::Auth("消息认证码无效".into()))
}

pub fn derive_local_key(install_salt: &[u8]) -> [u8; 32] {
    let machine = machine_uid::get().unwrap_or_else(|_| "lanflow-unknown-machine".into());
    let mut material = machine.into_bytes();
    material.extend_from_slice(install_salt);
    blake3::derive_key("LanFlow local credential obfuscation v1", &material)
}

pub fn obscure_password(key: &[u8; 32], password: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let cipher = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|error| LanFlowError::Internal(error.to_string()))?;
    let mut nonce = [0u8; 24];
    rand::rng().fill(&mut nonce);
    let nonce_array = XNonce::try_from(nonce.as_slice())
        .map_err(|_| LanFlowError::Internal("本地凭据 nonce 无效".into()))?;
    let ciphertext = cipher
        .encrypt(&nonce_array, password.as_bytes())
        .map_err(|_| LanFlowError::Internal("本地凭据混淆失败".into()))?;
    Ok((nonce.to_vec(), ciphertext))
}

pub fn reveal_password(key: &[u8; 32], nonce: &[u8], ciphertext: &[u8]) -> Result<String> {
    if nonce.len() != 24 {
        return Err(LanFlowError::Auth("本地凭据 nonce 无效".into()));
    }
    let cipher = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|error| LanFlowError::Internal(error.to_string()))?;
    let nonce_array =
        XNonce::try_from(nonce).map_err(|_| LanFlowError::Auth("本地凭据 nonce 无效".into()))?;
    let plaintext = cipher
        .decrypt(&nonce_array, ciphertext)
        .map_err(|_| LanFlowError::Auth("本地凭据已失效".into()))?;
    String::from_utf8(plaintext).map_err(|_| LanFlowError::Auth("本地凭据编码无效".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_registration_and_login_match_session_keys() {
        let setup = new_server_setup();
        let record = register_password(&setup, "share", "correct horse").unwrap();
        let (request, client) = begin_client_login("correct horse".into()).unwrap();
        let (response, server) = begin_server_login(&setup, "share", &record, &request).unwrap();
        let (finish, client_key) = client.finish(&response).unwrap();
        let server_key = server.finish(&finish).unwrap();
        assert_eq!(client_key, server_key);
    }

    #[test]
    fn local_obfuscation_roundtrip() {
        let key = derive_local_key(b"01234567890123456789012345678901");
        let (nonce, ciphertext) = obscure_password(&key, "secret").unwrap();
        assert_eq!(
            reveal_password(&key, &nonce, &ciphertext).unwrap(),
            "secret"
        );
    }
}

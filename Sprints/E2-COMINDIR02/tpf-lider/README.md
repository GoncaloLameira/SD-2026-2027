
# TPF Leader — Sistemas Distribuídos

Aplicação Líder do Trabalho Prático Final da unidade curricular de Sistemas Distribuídos.

O projeto utiliza um bot Discord para receber e arquivar mensagens, atribuindo identificadores sequenciais (LSN) e garantindo persistência local antes de confirmar os registos.

## Sprint 1 — Integração com Discord

O Sprint 1 estabeleceu a estrutura inicial do bot, a ligação ao servidor Discord e o registo dos comandos slash.

Comandos disponíveis na interface:

- `/registar texto` — regista um texto.
- `/estado` — consulta do estado (ainda não implementado).
- `/config r` — configuração (ainda não implementada).
- `/recuperar de ate` — recuperação por intervalo (ainda não implementada).
- `/replica-token` — gestão de réplicas (ainda não implementada).
- `/replica-remover id` — remoção de réplicas (ainda não implementada).
- `/rajada n intervaloms` — geração automática de registos.

A implementação da persistência e do comando `/rajada` foi desenvolvida no Sprint 2.

## Sprint 2 — Persistência e Arquivo Cifrado

### Objetivos

Implementar um sistema de arquivo com:

- Identificadores LSN sequenciais e persistentes.
- Escrita durável em `registos.log`.
- Recuperação após reinício.
- Agrupamento de registos em chunks.
- Selagem automática por tamanho ou tempo.
- Cifra AES-256-GCM.
- Derivação de chaves por HKDF-SHA256.
- Armazenamento local dos chunks cifrados.
- Deteção de alterações nos dados cifrados.
- Geração de rajadas para testes de carga.

### Arquitetura

Fluxo de processamento:

```text
Discord
   |
   v
BotListener
   |
   v
RegistoLog
   |----> registos.log
   |
   v
ChunkManager
   |
   v
Chunk
   |
   v
ChunkStorage
   |
   v
ChunkCrypto
   |----> HKDF-SHA256
   |----> AES-256-GCM
   |
   v
SealedStore
   |----> selados/<chunkId>.bin
   |----> selados/<chunkId>.hdr
```

No arranque, o `ChunkRecovery` verifica os chunks armazenados e compara os registos recuperados com o `registos.log`.

Os LSN ainda não encontrados em chunks válidos são reencaminhados para o `ChunkManager`.

### Componentes Java

| Classe | Responsabilidade |
|---|---|
| `LeaderBot` | Inicialização do sistema e ligação ao Discord |
| `BotListener` | Processamento de comandos e mensagens |
| `Wire` | Parâmetros de tamanho, padding e tempo |
| `MasterKey` | Criação e carregamento da chave mestra |
| `Registo` | Representação e serialização dos registos |
| `RegistoLog` | Persistência dos registos e gestão dos LSN |
| `Chunk` | Agrupamento, serialização e padding |
| `ChunkManager` | Gestão e selagem automática dos chunks |
| `ChunkCrypto` | Derivação de chaves, cifra e decifra |
| `Sealed` | Representação dos chunks cifrados |
| `SealedStore` | Armazenamento dos ficheiros cifrados |
| `ChunkStorage` | Integração entre cifra e armazenamento |
| `ChunkRecovery` | Identificação dos registos já arquivados |
| `ChunkCryptoTest` | Testes de cifra, recuperação e integridade |

## Persistência dos Registos

Os registos contêm:

- `lsn` — identificador sequencial.
- `timestamp` — instante de criação.
- `autor` — identificador do autor.
- `canal` — identificador do canal.
- `texto` — conteúdo da mensagem.

Os registos são escritos em `registos.log` através de `FileChannel`.

A chamada `force(true)` solicita a sincronização dos dados com o armazenamento antes da confirmação ao Discord.

Depois de reiniciar, o sistema lê o log e recupera o último LSN.

Os campos textuais são serializados em Base64 para preservar o formato das linhas. Base64 não é uma cifra.

## Agrupamento e Selagem de Chunks

Parâmetros definidos em `Wire.java`:

| Parâmetro | Valor |
|---|---|
| `CHUNK_MAX_BYTES` | 65 536 bytes |
| `PAD_BYTES` | 4096 bytes |
| `SELAGEM_MS` | 5000 ms |

Cada chunk contém vários registos serializados.

A selagem ocorre quando o chunk atinge a capacidade permitida ou quando decorre o intervalo configurado desde a entrada do primeiro registo.

Antes da cifra, é acrescentado padding com zeros até o tamanho ser múltiplo de 4096 bytes.

Um chunk vazio não é selado.

## Cifra dos Chunks

A cifra utiliza:

- Chave mestra de 32 bytes (`master.key`).
- Salt aleatório de 32 bytes por chunk.
- HKDF-SHA256 para derivar uma chave por chunk.
- AES-256-GCM para cifra autenticada.
- Nonce aleatório de 12 bytes.
- Tag de autenticação de 128 bits.
- SHA-256 dos dados cifrados para calcular o `chunkId`.

Os metadados utilizados na autenticação GCM incluem a versão da chave, o salt e o nonce.

A chave mestra não é incluída nos ficheiros dos chunks.

### Armazenamento

Cada chunk produz dois ficheiros:

```text
selados/
├── <chunkId>.bin
└── <chunkId>.hdr
```

O `.bin` contém os dados cifrados e a tag GCM.

O `.hdr` contém a versão da chave, o salt e o nonce.

## Recuperação após Reinício

O sistema executa os seguintes passos:

1. Carrega a chave mestra.
2. Lê os registos do `registos.log`.
3. Percorre os chunks existentes.
4. Verifica o identificador SHA-256 e a autenticação GCM.
5. Reconstrói os registos dos chunks.
6. Compara-os com o log persistente.
7. Identifica os LSN ainda não arquivados.
8. Reencaminha os registos pendentes para selagem.

Os testes realizados confirmaram a recuperação de 517 registos sem reencaminhamento desnecessário.

## Configuração

### Requisitos

- Java 25.
- Maven.
- IntelliJ IDEA ou outro ambiente compatível.
- Bot Discord configurado com as permissões necessárias.
- Acesso ao servidor Discord de testes.

### Ficheiro .env

Criar um ficheiro `.env` na raiz do projeto:

```env
DISCORD_TOKEN=<token_do_bot>
```

O token deve ser mantido privado.

### Chave mestra

Na primeira execução, é criada automaticamente uma chave mestra de 32 bytes no ficheiro `master.key`.

Nas execuções seguintes, a chave existente é reutilizada.

Não eliminar a chave caso existam chunks cifrados que dependam dela.

## Execução

Abrir o projeto Maven no IntelliJ e executar:

```text
tpf.LeaderBot
```

O bot liga-se ao servidor Discord configurado e regista os comandos disponíveis.

As mensagens normais publicadas no canal `#arquivo` são igualmente encaminhadas para o sistema de persistência.

### Comando /registar

Exemplo:

```text
/registar texto:Mensagem de teste
```

O bot confirma o registo e apresenta o LSN atribuído.

### Comando /rajada

O comando exige o cargo Discord `admin-arquivo`.

Exemplo de teste pequeno:

```text
/rajada n:10 intervaloms:100
```

Exemplo de teste de carga:

```text
/rajada n:500 intervaloms:0
```

A implementação limita as rajadas a 5000 registos e utiliza uma tarefa de execução separada para não bloquear os eventos do Discord.

## Testes Realizados

### Teste 1 — Persistência e LSN

Foram criados registos com LSN consecutivos.

Após reiniciar o bot, a numeração continuou a partir do último LSN persistido.

Resultado: aprovado.

### Teste 2 — Mensagens no canal #arquivo

Foi enviada uma mensagem normal ao canal `#arquivo`.

O bot registou a mensagem utilizando o mesmo mecanismo de persistência do comando `/registar`.

Resultado: aprovado.

### Teste 3 — Cifra AES-256-GCM

O `ChunkCryptoTest` verificou:

- Serialização e padding.
- Cifra e decifra.
- Gravação dos ficheiros `.bin` e `.hdr`.
- Recuperação dos três registos originais.
- Rejeição de ciphertext adulterado.
- Rejeição de chave incorreta.

Resultado: todos os testes passaram.

### Teste 4 — Agrupamento

Foram enviadas três mensagens rapidamente.

O sistema agrupou-as num único chunk de 4096 bytes antes da cifra.

Resultado: aprovado.

### Teste 5 — Rajada de 10 registos

Configuração:

```text
n=10
intervaloms=100
```

Resultados:

```text
Registos criados: 10
LSN: 8 a 17
Duracao: 1061 ms
```

Foi criado um chunk de 8192 bytes antes da cifra e 8208 bytes depois da cifra.

Resultado: aprovado.

### Teste 6 — Rajada de 500 registos

Configuração:

```text
n=500
intervaloms=0
```

Resultados:

```text
Registos criados: 500
LSN: 18 a 517
Duracao: 3523 ms
Chunks gerados: 4
```

Distribuição observada:

| Chunk | Registos | Bytes antes da cifra |
|---|---:|---:|
| 1 | 145 | 65 536 |
| 2 | 144 | 65 536 |
| 3 | 144 | 65 536 |
| 4 | 67 | 32 768 |

Nenhum dos chunks serializados ultrapassou o limite de 65 536 bytes.

Resultado: aprovado.

### Teste 7 — Recuperação dos 517 registos

Após reiniciar o bot, foi obtido:

```text
RECUPERACAO | registos no log=517 | ja arquivados=517
RECUPERACAO CONCLUIDA | pendentes encaminhados=0
```

Resultado: aprovado.

### Teste 8 — Verificação dos ficheiros

Foram identificados:

```text
Ficheiros BIN: 9
Ficheiros HDR: 9
BIN sem HDR: 0
HDR sem BIN: 0
```

Os nove pares incluem um chunk criado pelo teste de cifra, separado dos chunks correspondentes aos 517 registos reais.

Resultado: aprovado.

### Teste 9 — Deteção de corrupção

Foi alterado um byte de um ficheiro `.bin`.

Ao iniciar o sistema, o `ChunkRecovery` detetou a alteração e recusou continuar o arranque.

O ficheiro original foi posteriormente restaurado a partir de uma cópia de segurança.

Após a restauração, o sistema voltou a reconhecer os 517 registos.

Resultado: aprovado.

## Segurança e Limitações

Os seguintes ficheiros e pastas estão excluídos pelo `.gitignore`:

- `.env`
- `master.key`
- `registos.log`
- `eventos-lider.txt`
- `selados/`
- `backup-sprint2/`

Estas exclusões evitam a inclusão normal de ficheiros não rastreados no Git, mas não substituem os controlos de acesso do sistema operativo.

### Limitações conhecidas

- O `registos.log` mantém os conteúdos textuais em Base64, não cifrados. Por isso, o sistema ainda não assegura confidencialidade integral dos dados locais.
- A gravação do `.bin` e do `.hdr` não constitui uma transação atómica única.
- A recuperação de ficheiros incompletos ou órfãos após falhas durante a escrita necessita de tratamento adicional.
- A fila de chunks pendentes do `ChunkManager` reside em memória; a recuperação após reinício baseia-se no `registos.log` e nos chunks válidos encontrados.
- A proteção das permissões da `master.key` em Windows necessita de revisão.
- Os testes de carga apresentados foram realizados num ambiente local específico e não constituem uma garantia geral de desempenho.

## Estado de Desenvolvimento

O Sprint 2 implementa e demonstra o fluxo de receção, persistência, agrupamento, cifra e recuperação de registos.

Os testes funcionais descritos foram executados com sucesso.

A implementação ainda requer revisão dos cenários de falha e das garantias de confidencialidade para utilização fora do ambiente de testes.

## Testes adicionais de segurança e robustez

### Autenticacao dos metadados

Foram realizados testes de adulteracao dos seguintes elementos:

- Ciphertext
- Chave mestra
- Salt
- Nonce
- KeyVersion

Todos os dados adulterados foram rejeitados durante a decifra.

### Evento SELADO

Foi confirmado o registo persistente do evento `SELADO`
no ficheiro `eventos-lider.txt`.

O evento inclui:

- chunkId
- LSN inicial e final
- Quantidade de registos
- Tamanho original
- Tamanho cifrado

### Deteção de chunks incompletos

Foi criado um ficheiro `.bin` ficticio sem o respetivo `.hdr`.

O sistema detetou o ficheiro incompleto e impediu
o arranque do bot.

Apos remover o ficheiro de teste, a recuperacao
voltou a funcionar normalmente.

### Validacao final

Resultados obtidos:

- Registos persistidos: 518
- Registos reconhecidos como arquivados: 518
- Registos pendentes: 0
- Ligacao ao Discord: bem-sucedida

Os testes validam os cenarios executados, mas nao
garantem recuperacao automatica de todas as falhas
possiveis durante a escrita.

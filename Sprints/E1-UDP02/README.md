## 10. Tabela de resultados

| Mensagem enviada | Resposta recebida | L após processamento | Estrutura temporária | Mensagens entregues neste passo | Justificação |
|---|---|---:|---|---|---|
| `1, olá` | `1, olá` | 1 | `{}` | `1, olá` | É a próxima mensagem esperada, por isso é entregue imediatamente. |
| `3, mundo` | `waitingfor,2` | 1 | `{3=3,mundo}` | Nenhuma | Falta a mensagem 2, por isso a mensagem 3 fica guardada temporariamente. |
| `4, tudo bem` | `waitingfor,2` | 1 | `{3=3,mundo, 4=4,tudo bem}` | Nenhuma | Continua a faltar a mensagem 2, por isso a mensagem 4 também fica guardada. |
| `2, cruel` | `2, cruel` | 4 | `{}` | `2, cruel`; `3, mundo`; `4, tudo bem` | A mensagem 2 é entregue e permite entregar em cascata as mensagens 3 e 4 que estavam guardadas. |
| `3, mundo` | `waitingfor,5` | 4 | `{3=3,mundo}` | Nenhuma | A mensagem 3 já tinha sido entregue. O algoritmo atual não deteta este duplicado e guarda-o na estrutura temporária. |
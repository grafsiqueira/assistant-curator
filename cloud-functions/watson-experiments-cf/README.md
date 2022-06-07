# Métricas geradas pelo watson-assistant-experiment:

## **overview:**

Resumo geral do desempenho do assistente:

- weighted_precision;
- weighted_recall;
- weighted_f1;

### Exemplo:

- **weighted_recall baixo**: muitas classes de intenções com poucos exemplos.

- **weighted_precision baixa** e **weighted_recall** dentro do esperado: desbalanço entre o tamanho das classes.

---

## **class_distribution:**

Retorna a quantidade de exemplos fornecidos para cada intent criada no assistente consultado.

---

## **precision_at_k:**

- **Obs.:** Entender "precisão" como “taxa de acerto”

Métrica que exibe a precisão do assistente em determinar a intent correta, considerando a posição que a mesma ocupa na ordem de confiança retornada pelo assistente.

### Exemplo:

        {
            “k”: 0,
            “precision”: 0.8084
        }

O assistente acerta a intent identificada "de primeira" 80,84% das vezes

        {
            “k”: 1,
            “precision”: 0.9305
        }

A intent certa para a frase recebida está entre as duas primeiras opções fornecidas pelo assistente 93% das vezes

…e assim em diante.

---

## **class_accuracy:**

Para cada uma das intents definidas, retorna:

- **precision:** “De todas que eu chutei, quantas eu acertei?”;

- **recall:** “De todas que tinham para acertar, quantas eu acertei?”;

- **f1:** média harmônica entre os dados anteriores;

### Exemplo:

Tenho 13 exemplos de uma classe, chutei que um input seria ela 10 vezes, acertando 7:

- **precision:** 7 ÷ 10
- **recall:** 7 ÷ 13

---

## **pairwise_class_errors:**

A ideia desse relatório é mostrar erros comuns que o assistente comete ao confundir duas intenções.

- **true_class:** intent correta para o input fornecido;

- **predicted_class class:** intent que o assistente (incorretamente) achou que o input queria dizer;

- **count:** quantidade de exemplos que foram entendidos incorretamente;

- **avg_confidence:** qual a confiança que foi dada para essa intent interpretada incorretamente;

- **errors:** array dos erros cometidos. Exemplo:

        {
            "predicted_class": "xingamento",
            "confidence": 0.08723873138427735,
            "input": {
                "text": "Yes"
            }
        }

---

## **accuracy_vs_coverage:**

Para cada nível de confiança (confidence threshold, conform sintaxe utilizade pelo Watson Assistant), essa métrica retorna qual seria sua acurácia e qual seria sua cobertura.

O threshold define qual é o limite de confiança aceitável para que o assistente não caia no “anything_else”: para qualquer mensagem que a confiança caia abaixo do threshold definido, o assistente escolhe não responder.

A ideia é permitir a análise de cada um dos cenários para poder decidir o que é melhor para cada o caso de uso do assistente.

- **accuracy:** quantas vezes seu assistente acertaria a intent que input queria dizer

- **coverage**: o quanto seu assistente saberia responder de todas as perguntas que foram feitas

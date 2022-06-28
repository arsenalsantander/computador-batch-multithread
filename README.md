# Computador Batch

Aplica��o batch baseado na arquitetura [Computador](http://confluence.notebookbr.corp/display/ARSENALCNA).

Esse projeto foi gerado automaticamente pelo `computador-archetype-batch`.


* [Introdu��o](#introdu��o)
  + [Objetivo](#objetivo)
  + [Evolu��o](#evolu��o)
* [Instala��o](#instala��o)
  + [Pr�-requisitos](#pr�-requisitos)
  + [Configura��o da Aplica��o](#configura��o-da-aplica��o)
    - [Perfil `default`](#perfil-default)
    - [Perfil `local`](#perfil-local)
* [Utiliza��o](#utiliza��o)
  + [Compila��o](#compila��o)
  + [Execu��o da Aplica��o](#execu��o-da-aplica��o)
* [Documenta��o](#documenta��o)
* [Contribui��es](#contribui��es)
* [Licen�a](#licen�a)

> TODO: descreva o contexto e hist�rico que motivaram a constru��o dessa aplica��o batch.

> TODO: descreva o objetivo e principais funcionalidades contempladas por essa aplica��o batch

> TODO: descreva as potenciais funcionalidades que ainda ser�o desenvolvidas nessa aplica��o batch 


Baixe o c�digo-fonte desse reposit�rio usando `git clone` ou de forma manual (.zip).
Para prosseguir com a insta��o deste projeto � necess�rio atender a tabela de pr�-requisitos com componentes na vers�o
especificada ou superior.


| Componente   | Vers�o       | Descri��o                      |
|--------------|--------------|--------------------------------|
| OpenJDK      | 11.0.9.1     | Plataforma Java                |
| Apache Maven | 3.6.1        | Build e Gest�o de Depend�ncias |

> TODO: inclua as demais depend�ncias e tecnologias do seu projeto (ex: Redis, Oracle, RabbitMQ, etc.)


A aplica��o foi planejada para ser executada em dois ambientes distintos compartilhando o m�ximo de similaridades poss�veis.
Para isso foram criados dois perfis (_profiles_). Detalhes das configura��es, profiles e sua utiliza��o est�o na pr�xima se��o.


Por padr�o a aplica��o est� configurada para executar no ambiente de produ��o. Portanto, o _profile_ `default`
refere-se as configura��es que ser�o utilizadas durante a opera��o real do sistema.


Um segundo _profile_ `local` est� definido e sobreescreve os valores de algumas configura��es do _profile_ `default`
para que a aplica��o execute corretamente no ambiente local do desenvolvedor.


Abaixo seguem instru��es de uso do projeto. Os comandos devem ser executados no terminal, ou diretamente na IDE de prefer�ncia
do desenvolvedor.


Para fazer o download de depend�ncias e compilar o c�digo-fonte da aplica��o, navegue at� o diret�rio raiz do projeto execute:

```bash
mvn clean package
```


Para executar a aplica��o em modo de desenvolvimento, o _profile_ `local` deve ser ativado explicitamente (os exemplos dados s�o com Maven, Spring e Java, especificamente nessa ordem):

```bash
mvn spring-boot:run -Dspring.profiles.active=local
```

```bash
spring-boot:run --spring.profiles.active=local
```

```bash
java -jar target/computador-batch-demo.jar --spring.profiles.active=local
```

A aplica��o poder� ser executada diretamente pela IDE, ou diretamente numa estrutura identica aos servidores Linux/Windows atrav�s de scripts (.bat e .sh).

https://confluence.notebookbr.corp/display/ARSENALCNA/Projeto+Computador+Batch.

Para reportar _bugs_, sugerir novas funcionalidades e melhorias entre em contato com alguns dos membros respons�veis
por este projeto, ou submeta um _Pull Request_ seguindo as pr�ticas e conven��es de desenvolvimento desse projeto.
> TODO: inclua detalhes adicionais de como outras pessoas podem contribuir com esse projeto.

Direitos reservados do Grupo Notebook.

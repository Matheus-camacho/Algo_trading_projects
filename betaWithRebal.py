from dorothy import StrategyBase
from datetime import timedelta
from numpy import arange
from scipy import stats
import pandas as pd

'''
!!! ATENÇÃO !!!
RECOMENDAMOS DEIXAR A FREQUENCIA EM ['1' 'DAY']. ESSA ESTRATÉGIA CONSOME MUITOS ATIVOS.
'''


class Strategy(StrategyBase):
  '''
  Tipo:                Momentum
  Universo de Ativos:  Empresas brasileiras com Market Cap superior a R$ 1,000,000,000.00
  Regra de negócio:    A ideia dessa estratégia é calcular o coeficiente angular de todos os ativos e fazer um ranking pelos maiores e menores coeficientes.
  Compramos os 5% maiores e vendemos os 5% menores por 3 meses e rebalanceamos a cada 3 meses, fechando a posição atual e abrindo novas.
  '''

  def on_init(self, settings):
    """ Evento chamado na inicialização da estratégia. """
    
    # Selecionando settings de acordo com sugestões da plataforma
    settings.default_set = 'BR'
    settings.default_set = 'impact'

    # Percentual do ranking a ser selecionado para compra e venda. 5%.
    self.percentile = 0.05
    
    # Quantidade de dados passados que serão utilizados para calcular o ranking de ativos
    self.lookback_length = 30

    # Valor do capital inicial para dividir em partes iguais para cada posição aberta.
    self.init_cap = self.position.balance(total=True)

    # Usamos a função 'data.get.bar_close_tickers' para pegar os 100 instrumentos mais líquidos do setor financeiro na B3.
    self.tickers = self.data.get.bar_close_tickers(exchanges='B3', by='liquidity', sectors='Financial', top=100)

    # Faz o subscribe de todos os ativos para a estratégia e retorna a lista de objetos 'instrument_info'.
    # O parâmetro lookback_length faz com o que o campo lookback no objeto 'instrument_info' carregue sempre as 30 barras mais recentes do ativo.
    self.instruments = self.data.subscribe.bar_close(self.tickers, lookback_length=self.lookback_length)

    # Agendando a execução da função trade_logic (definida abaixo) para 5 minutos antes da abertura do mercado a cada 90 dias com recorrencia até o fim do backtest.
    # Essa função vai chamar a função 'self.trade_logic'.
    self.time.schedule_function(
      function=self.trade_logic,
      start_on='market_open',
      offset=-timedelta(minutes=5),
      step=timedelta(days=90),
      repeats_till_cancel=True
    )

  def trade_logic(self):
    '''
    Executa as aberturas e fechamentos de posições na seguindo a lógica da estratégia
    '''
    
    # Fechando as posições existentes, caso existam, antes de abrir novas posições.
    self.close_positions()

    # Calculando o ranking atual dos instrumentos assinados
    buy_tickers, sell_tickers = self.ranking()
    
    # Calculando a quantidade de ativos a serem usados
    tickers_qty = len(buy_tickers) + len(sell_tickers)

    # Confere se há ativos para abrir posição.
    if tickers_qty > 0:
      # Calcula o financeiro a ser usado para cada instrumento.
      slice_amt = self.init_cap / tickers_qty

      # Gera um output para cada rebalanceamento.
      print('\n\nTickers selecionados - %s:' % self.time.current_time)
      print('Compra: %s' % buy_tickers)
      print('Venda: %s' % sell_tickers)

      # Loop nos ativos selecionados para COMPRA.
      for ticker in buy_tickers:
        # Envia uma ordem de COMPRA a mercado com duração 'GOOD_TILL_CANCEL'.
        self.order.create.market(
          instrument=self.instruments[ticker],
          side='BUY',
          amount=slice_amt,
          time_in_force=self.order.fields.time_in_force.GOOD_TILL_CANCEL
        )
        
      # Loop nos ativos selecionados para VENDA.
      for ticker in sell_tickers:
        # Envia uma ordem de VENDA a mercado com duração 'GOOD_TILL_CANCEL'.
        self.order.create.market(
          instrument=ticker,  # O próprio ticker pode ser passado como argumento em vez o objeto 'instrument_info'
          side='SELL',
          amount=slice_amt,
          time_in_force='GTC'  # GOOD TILL CANCEL
        )

  def close_positions(self):
    '''
    Busca todas as posições abertas (Compra e Venda) e as fecha enviando ordens a mercado.
    '''

    # Busca a quantidade de ações possuídas em todos os ativos assinados.
    # Ativando tanto o filtro filter_long com o filter_short, eliminamos do retorno da função as posições nulas.
    positions = self.position.size(filter_long=True, filter_short=True)

    # Loop em cada instrumento com posição aberta.
    # Como a variavel positions é um dicionário, possui sempre uma chave e um valor como no exemplo seguinte: { 'BPAC11': 1000 }. 'BPAC11' é o ticker do instrumento e o 1000 é a quantidade de ações possuída.
    for key, value in positions.items():
      # Caso o 'value' (quantidade de ações) seja menor que zero, definimos o 'side' da ordem como 'BUY' (compra) para fazer a operação inversa, fechando a posição.
      if value < 0:
        side = 'BUY'
      # Caso o 'value' (quantidade de ações) seja maior que zero, definimos o 'side' da ordem como 'SELL' (venda) para fazer a operação inversa, fechando a posição.
      elif value > 0:
        side = 'SELL'
        
      # Calcula o tamanho da ordem (quantidade de ações) buscando a posição em aberto em módulo (sempre positivo)
      size = abs(value)

      # Envia uma ordem a mercado que duração indeterminada ('GOOD_TILL_CANCEL').
      self.order.create.market(
        instrument=key,
        side=side,
        size=size,
        time_in_force=self.order.fields.time_in_force.GOOD_TILL_CANCEL
      )
        
  def ranking(self):
    '''
    Calcula o coeficiente angular dos últimos 30 dias para todos
    os instrumentos e faz um ranqueamento dos top 5% e bottom 5%.
    '''
    
    # Cria um dicionario a ser preenchido com o ranking.
    ranking = {}

    # Loop nos ativos assinados no começo da estratégia.
    for ticker, ticker_info in self.instruments.items():
      # Recuperando os 30 últimos preços de VWAP do ativo, armazenados no lookback dos objetos 'instrument_info'.
      df = ticker_info.lookback('vwap')

      # Caso a quantidade de linhas do dataframe seja diferente de 30 (30 dias), seguir para o proximo instrumento.
      if len(df) != self.lookback_length:
        continue

      # Cria um array sequencial com a mesma quantidade de linhas do dataframe (30).
      seq = arange(0, self.lookback_length)

      # Calcula os dados da regressão linear do instrumento selecionado.
      slope, intercept, r_value, p_value, std_err = stats.linregress(seq, df)

      # Insere o coeficiente angular (slope) em um dicionario com o nome do ticker como chave.
      ranking[ticker] = slope

    # Calcula a quantidade de ativos para cada ponta (top e bottom) dado o percentual de 5%.
    num_tickers = int(len(ranking) * self.percentile)

    # Checa se num_tickers é maior ou igual a 1 para garantir que pelo menos 1 ativo pode ser selecionado do universo estudado.
    if num_tickers >= 1:
      # Transforma o dicionário em um dataframe para facilitar o manuseamento dos dados
      ranking = pd.DataFrame(ranking, index=['slope']).transpose()

      # Orderna o dataframe em ordem decrescente pelo slope.
      ranking = ranking.sort_values('slope', ascending=False)
      
      # Usando o método head de DataFrame para pegar os primeiros itens do ranking.
      buy_tickers = ranking.head(num_tickers)
      
      # Deixando apenas slopes positivos no buy_tickers
      buy_tickers = buy_tickers.loc[buy_tickers['slope'] > 0]
      
      # Recuperando apenas os tickers (índices do DataFrame) listados no buy_tickers em formato de lista.
      buy_tickers = list(buy_tickers.index.values)

      # Usando o método tail de DataFrame para pegar os últimos itens do ranking.
      sell_tickers = ranking.tail(num_tickers)
      
      # Deixando apenas slopes negativos no sell_tickers
      sell_tickers = sell_tickers.loc[sell_tickers['slope'] < 0]
      
      # Recuperando apenas os tickers (índices do DataFrame) listados no sell_tickers em formato de lista.
      sell_tickers = list(sell_tickers.index.values)

      # Retorna as duas listas com os intrumentos a serem comprados e vendidos.
      return buy_tickers, sell_tickers
    
    # Caso a variavel num_tickers seja menor que 1, essa função retorna listas vazias. Nesse caso nenhum ativo será negociado.
    else:
      return [], []

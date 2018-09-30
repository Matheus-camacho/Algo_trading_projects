from dorothy import StrategyBase
from finmath import ta


class Strategy(StrategyBase):
  '''
  Tipo:                Mean Reversion
  Regra de negócio:    Vender um ativo se for detectado que está tendo um aumento de preço atípico ou comprá-lo se for detectado uma queda atípica de preço.
  '''
  
  def on_init(self, settings):
    """ Evento chamado na inicialização da estratégia. """
    
    # Utilizando configurações sugeridas para o cenário brasileiro (custos de comissão, alocações automáticas de caixa parado em CDI, etc)
    settings.default_set = 'br'
    
    # Limitando quantidade máxima de ordens ativas simultaneamente
    settings.general.max_num_orders = 4
    
    # Definindo parâmetros básicos da estratégia
    # Ticker que será analisado
    self.ticker = 'BBDC3'
    # Número de barras que serão usadas para montar as BBands
    self.period = 20
    # Zscore que gera o sinal de entrada
    self.zscore = 2
    # Stop loss das bracket orders (%)
    self.stop_loss = 2
    # Take profit das bracket orders (%)
    self.take_profit = 1
    # Quantidade de R$ que será investida a cada sinal de abertura
    self.bet_size = self.position.balance(total=True) / 10
    
    # Criando as BBands
    self.bbands = ta.bbands(max_len=self.period, zscore=self.zscore)
    
    # Inicializando as BBands com os últimos closes
    lookback = self.data.get.hist.bar_close(self.ticker, length=self.period)['close']
    for bar_close in lookback:
      self.bbands.add(bar_close)
    
    # Assinando o ticker para podermos receber seus preços de fechamento a cada loop
    self.instrument = self.data.subscribe.bar_close(self.ticker)
    print(self.instrument)
    
    # Criando variáveis de controle para análise posterior da estratégia
    self.num_take_profit = 0
    self.num_stop_loss = 0
    self.order_ids = set()

  def on_bar_close(self, bar):
    """ Evento de captura de preços """
    
    # Inserindo o novo preço nas BBands
    self.bbands.add(bar.close)
    
    # Se o preço de fechamento ultrapassou a BBand superior, fazer um short no ativo
    if bar.close >= self.bbands.upper_band:
      print('Bar close > Upper band: SHORT')
      self.order.send.bracket(
        instrument=self.instrument,
        side='SHORT_SELL',
        stop_loss=self.stop_loss,
        take_profit=self.take_profit,
        amount=self.bet_size,
        limit=bar.close,
        time_in_force='GTC'  # GOOD TILL CANCEL
      )

    # Se o preço de fechamento está abaixo da BBand inferior, comprar o ativo
    elif bar.close <= self.bbands.lower_band:
      print('Bar close < Upper band: LONG')
      self.order.send.bracket(
        instrument=self.instrument,
        side='BUY',
        stop_loss=self.stop_loss,
        take_profit=self.take_profit,
        amount=self.bet_size,
        limit=bar.close,
        time_in_force='GTC'  # GOOD TILL CANCEL
      )

  def on_order_status(self, exec_info):
    """ Evento de captura de estados das ordens ativas """
    
    # Imprimindo as atualizações das ordens
    print(exec_info)
    
    # Contando quantidade de take-profits e stop-losses emitidos (os IDs são armazenados para que a mesma ordem não seja observada mais de uma vez)
    if exec_info.id not in self.order_ids and ' - ' in exec_info.order_type:
      self.order_ids.add(exec_info.id)
      if exec_info.order_type.endswith('take-profit'):
        self.num_take_profit += 1
      elif exec_info.order_type.endswith('stop-loss'):
        self.num_stop_loss += 1
  
  def on_exit(self):
    """ Evento usado para a finalização da estratégia. Negociação não habilitada """
    
    # Imprimindo informações para análise da estratégia
    print('%d take-profits foram emitidos' % self.num_take_profit)
    print('%d stop-losses foram emitidos' % self.num_stop_loss)
    
    # Calculando quantidade total de take-profits e stop-losses
    num_total = self.num_take_profit + self.num_stop_loss
    
    # Se o total for maior que 0, as proporções de stop-loss e take-profit podem ser calculadas
    if num_total > 0: 
      print('%.2f das ordens foram take-profits' % (self.num_take_profit / num_total))
      print('%.2f das ordens foram stop-losses' % (self.num_stop_loss / num_total))

      # Se mais da metade das ordens teve stop-loss, gerar um aviso
      if (self.num_stop_loss / num_total) > 0.5:
        self.custom_warning('Mais de metade das ordens teve eventos de stop-loss')

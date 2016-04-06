module BlacklightOaiProvider
  class SolrDocumentWrapper < ::OAI::Provider::Model
    attr_reader :model, :timestamp_field
    attr_accessor :options
    def initialize(controller, options = {})
      @controller = controller

      defaults = { :timestamp => 'timestamp', :limit => 15}
      @options = defaults.merge options

      @timestamp_field = @options[:timestamp]
      @limit = @options[:limit]
      puts @options
    end

    def sets
    end

    def earliest
      Time.parse @controller.get_search_results(@controller.params, {:fl => @timestamp_field, :sort => @timestamp_field +' asc', :rows => 1}).last.first.get(@timestamp_field)
    end

    def latest
      Time.parse @controller.get_search_results(@controller.params, {:fl => @timestamp_field, :sort => @timestamp_field +' desc', :rows => 1}).last.first.get(@timestamp_field)
    end

    def find(selector, options={})
      return next_set(options[:resumption_token]) if options[:resumption_token]

      if :all == selector
        puts @options
        puts @controller.params
        # 2016-04-06T17:09:49Z
        if @controller.params?(:from)
          @controller.params[:fq] << "system_create_dtsi:[" + @controller.params?(:from) + " TO NOW]"
          response, records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit, :fq => "system_create_dtsi:[" + @controller.params?(:from) + " TO NOW]"})
        else
          response, records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit})
        end

        if @limit && response.total >= @limit
          return select_partial(OAI::Provider::ResumptionToken.new(options.merge({:last => 0})))
        end
      else
        response, records = @controller.get_solr_response_for_doc_id selector.split('/', 2).last
      end
      records
    end

    def select_partial token
      records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit, :start => token.last}).last

      raise ::OAI::ResumptionTokenException.new unless records

      OAI::Provider::PartialResult.new(records, token.next(token.last+@limit))
    end

    def next_set(token_string)
      raise ::OAI::ResumptionTokenException.new unless @limit

      token = OAI::Provider::ResumptionToken.parse(token_string)
      select_partial(token)
    end
  end
end

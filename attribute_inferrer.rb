=begin
The AttributeInferrer can be used to infer field values for a
record given disparate, disjointed and incomplete datasets.
On a per-field basis, the user of AttributeInferrer defines
algorithms that:

* extract candidate values from the datasets
* score those candidate values
* canonicalize those candidate values for final assignment

Given these algorithms, the AttributeInferrer can apply a
a weighted scoring algorithm that is capable of selecting
the best available value for each field.

For instance, suppose that given a person's name and zip code,
you are inferring their email address, home address and phone
number from:

* scraped social media accounts
* voter registration records
* a digital phonebook

```ruby
  class PersonAttributeInferrer
    include ::AttributeInferrer

    attr_reader :person

    # Our inferrer will need to have some starter data --
    # the person's name and zip code -- in order to
    # correctly make query datasets. We assume that those
    # values are encapsulated in @person.
    def initialize(person)
      @person = person
    end

    # The outermost layer of the DSL.
    infers do
      # #helper creates a method that is available from within #dataset,
      # #candidates, #canonicalize, #prefer and #score blocks.  At runtime,
      # the code defined within these blocks is executed within the
      # context of dynamically-created "evaluator" classes. (These classes
      # hold state and provide performance improvements via memoization
      # and a few other small, stateful optimizations.)
      #
      # In this case, we define a helper predicate method that returns a
      # truthy value if the phone number is of a valid format, else returns
      # a falsy value.
      helper :valid_phone_number? do |phone_number|
        canonicalize_phone_number(phone_number) =~ /\d{3}\.\d{3}\.\d{4}/
      end

      # A helper to put phone numbers into a "clean" form. See #canonicalize
      # below for more on how this would be used.
      helper :canonicalize_phone_number do |phone_number|
        # Implementation left as an exercise for the reader.
      end

      # Like #canonicalize_phone_number, but for addresses
      helper :canonicalize_address do |address|
        # Implementation left as an exercise for the reader.
      end

      # Like #canonicalize_phone_number, but for email addresses
      helper :canonicalize_email_address do |email|
        # Implementation left as an exercise for the reader.
      end

      # #share defines a helper that is simply delegated to this inferrer.
      # This should typically be used only for simple attr_readers.
      #
      # We want this inferrer's :person attr_reader to be accessible
      # via our code blocks below.
      share :person

      # #dataset blocks define collections of data that will be reused
      # in determining candidates for field values. When defining a
      # dataset, you will need to provide:
      # * a name (Symbols are preferred)
      # * a block which will yield a collection.
      #
      # When defining sources for fields, if the #source name is the
      # same as a defined #dataset name, the dataset can automatically
      # be accessed via the #dataset method. (This is particularly useful
      # in #candidate blocks. See #source and #candidate below for more
      # information.)
      #
      # We want to respect individuals' privacy, so we'll only
      # use social media accounts where the user has specified
      # that it is OK to contact them.
      dataset :social_media_accounts do
        # These blocks will be executed in the scope of this
        # inferrer, so it's OK to call methods on the inferrer.
        # In this case, we'll need to know the first and last
        # names of the person we're inferring data for.
        SocialMediaAccount.where(
          ok_to_contact: true,
          first_name: person.first_name,
          last_name: person.last_name
        )
      end

      dataset :voter_registrations do
        VoterRegistration
          .where(
            first_name: person.first_name,
            mi: person.middle_name[0],
            last_name: person.last_name,
            zip_code: person.zip
          )
      end

      # Since we're dealng with people, we want whitepages, not
      # yellow pages.
      dataset :phonebook_listings do
        PhonebookListing
          .where.not(business: true)
          .where(first_name: person.first_name)
          .where(last_name: person.last_name)
          .where(zip_code: person.zip_code)
      end

      # #field defines a named value for which this inferrer can
      # determine a best available value. In order to be meaningful,
      # a field must supply at least one #source block. It can
      # also provide #canonicalize and #prefer blocks.
      #
      # In our case, one of the values we are attempting to infer
      # is a given person's phone number.
      field :phone_number do

        # #source specifies a distinct algorithm (usually associated
        # with a dataset) for generating and scoring candidate
        # values. In order to be meaningful, it must have a
        # #candidates block and a #score block.
        #
        # In addition, the #source block should provide a weight,
        # where the weight is the given source's proportion of the
        # final score. (By convention, a source's weight is a value
        # between 0.0 and 1.0, and the sum of all weights for sources
        # within a given field is 1.0).
        #
        # For instance, suppose that for a given field Source A has
        # a weight of 0.60 and Source B has a weight of 0.40. Source
        # A's scores contain a single value "foo" with a score of 0.90.
        # Source B's scores contain a single value "bar" with a score
        # of 1.0. When the field calculates the overall scores, it will
        # give "foo" a score of 0.54 (0.90 * 0.60) and "bar" a score of
        # 0.40 (1.0 * 0.40).
        #
        # Candidate values appearing in multiple sources have their
        # scores summed after a weight is applied (the philosophy
        # being that two independent sources reporting the same value
        # is an indicator that the value is correct).
        #
        # Let's go back to Source A and Source B with weights 0.60 and
        # 0.40, respectively. Suppose that Source A reports two values:
        # "foo" with a score of 1.0 and "baz" with a score of 0.70.
        # Source B reports two values: "bar" with a score of 1.0 and "baz"
        # with a score of 0.90. When the field calculates scores, it
        # will calculate the following:
        # * "foo": 0.60
        # * "bar": 0.40
        # * "baz": 0.78 ((0.70 * 0.60) + (0.9 * 0.4))
        #
        # Even though "baz" didn't "win" for either source, it is
        # by far the overall winner.
        #
        # Back to our main example:
        # We're fairly confident that if someone has a phone
        # number listed on their social media accounts, that
        # is the number at which they want to be contacted.
        # We give candidates from this dataset 75% of the weight
        # in the final scoring algorithm for :phone_number.
        source :social_media_accounts, weight: 0.75 do

          # #candidates blocks provide a list of raw values that
          # should be used in calculating the score. These are
          # usually the values directly extracted from a dataset
          # (if one is provided) . See #canonicalize below for
          # more information.
          #
          # In this example, we want the distinct cell phone numbers
          # listed on social media accounts for the person in question.
          candidates do
            dataset
              .select(:cell_phone)
              .distinct
              .map(&:cell_phone)
          end

          # #score blocks provide a mechanism for evaluating the
          # quality of a group of candidates. Given a canonical
          # value and a list of its equivalent candidate values,
          # the score block should yield some numeric score. The
          # higher the score, the more fit the value.
          # * canonical_value: the value to which some group of candidates
          #     canonicalized. (See #canonicalize below.)
          # * equivalencies: a list of values, all of which canonicalize
          #     to the same canonical value. (See #canonicalize below.)
          # By convention, score blocks should yield a value between
          # 0.0 and 1.0.
          #
          # If it's a valid phone number, we treat it as a good
          # value (we give it a score of 1.0). Otherwise, we
          # hang onto the value but are highly suspicious (we
          # give it a score of 0.1).
          score do |canonical_value, _equivalencies|
            valid_phone_number?(canonical_value) ? 1.0 : 0.1
          end
        end

        source :phonebook_listings, weight: 0.25 do
          candidates do
            dataset.map(&:phone_number).uniq
          end

          # If the phonebook says that's the number, that's the
          # number. We give it a score of 1.0. Bear in mind,
          # however, that this will only result in an overall
          # bump of 0.25 for the given number's score, since
          # for this field, this dataset only has a weight of 0.25.
          #
          # This guarantees that if a number is in both datasets,
          # it will be given much higher priority than a phone
          # number only appearing in one. This will also guarantee
          # phonebook listings will not be given higher priority
          # than user-supplied contact information.
          score { |candidate| 1.0 }
        end

        # The canonicalize block tells the inferrer to group and
        # score values by their *canonical* value. Two values that
        # canonicalize to the same value will be treated as the same
        # value.
        #
        # Note that #score and #prefer blocks take as parameters
        # a canonicalized_value and a list of equivalencies. This is
        # because the results of the #candidates block are passed
        # through the #canonicalize block and grouped such that
        # any results which canonicalize to the same value are
        # treated as equivalencies under that canonicalized value.
        #
        # For instance if a #canonicalize block looks like this:
        # canonicalize { |string| string.upcase }
        #
        # and the corresponding #candidates block looks like this:
        # candidates { ['abc', 'Abc', 'ABC', 'bcd'] }
        #
        # The system will group the candidate values like this:
        # {
        #   'ABC': ['abc', 'Abc', 'ABC'],
        #   'BCD': ['bcd']
        # }
        #
        # When #prefer or #score blocks are run, they will receive
        # 'ABC' or 'BCD' as canononicalized candidate values and
        # ['abc', 'Abc', 'ABC'] or ['bcd'] (respectively) as their
        # equivalencies.
        #
        # #canonicalize blocks can be provided at the #field or #source
        # level:
        # * If a #source and it's #field have a #canonicalize block,
        #   the source will group using its block, and the field will
        #   group using its own block.
        # * If a #source has no #canonicalize block, it will use the
        #   #field's #canonicalize block.
        # * If a source provides a #canonicalize block but its field
        #   does not, the field will canonicalize using the default
        #   canonicalization block.
        #
        # The default #canonicalize block simply groups each candidate
        # value with itself.
        #
        # In our example:
        # We want the system to treat "(123) 456-7890" as the same
        # value as "123.456.7890". That is, independently of format,
        # we want the system to group and score phone numbers
        # according to the actual phone number, not the string
        # representing that phone number.
        canonicalize { |candidate| format_phone_number(candidate) }

        # #prefer blocks select the actual value that will be scored
        # for a source or field. Whereas #canonicalize is used to
        # group candidate values, #prefer is used to choose a best value.
        # Commonly, the best value set by a #prefer block is a raw
        # (un-canonicalized) value selected from the list of equivalencies.
        #
        # Supposing a canonicalized value of "ABC" and equivalencies
        # ['abc', 'Abc', 'ABC'], we might wish to choose the raw value
        # with the most uppercase characters:
        #
        # prefer do |canonical, equivalencies|
        #   equivalencies.max_by |raw_value|
        #     count_capital_letters(raw_value)
        #   end
        # end
        #
        # It is not a requirement that the #prefer block yield a value from
        # the list of equivalencies.
        # For instance, supposing we are attempting to create an acronym
        # with the canonicalized_candidate "ABC" and equivalencies
        # ['abc', 'Abc', 'ABC'], our prefer block might look like this:
        #
        # prefer do |canonical, _equivalencies|
        #   canonical.split('').map {|c| "#{c}."}.join
        # end
        #
        # Like #canonicalize, the prefer is executed at both the #source
        # level and the #field level.
        #
        # In our example, we wish to choose the raw value that is closest to
        # the canonicalized value:
        prefer do |canonical, equivalencies|
          equivalencies.min_by do |raw_value|
            Levenshtein.distance(canonical, raw_value)
          end
        end
      end

      field :email do
        canonicalize { |candidate| canonicalize_email_address(candidate) }

        # We assume that, for the purposes of extracting the
        # email address, social media accounts are the only dataset from
        # which we can extract an email address. We'll give that dataset
        # 100% of the weight in the final score for email.
        source :social_media_accounts, weight: 1.0 do
          # Gives us a list of all distinct email addresses in the
          # dataset.
          candidates do
            dataset
              .where.not(email: nil)
              .select(:email)
              .distinct
              .map(&:email)
          end

          # We generate a value -- by convention between 0.0 and 1.0 --
          # that represents the quality of the candidate.
          #
          # In this case, we say that if there is one element of the
          # dataset that has the given email address, the score is 0.5.
          # If there are two elements, the score is 0.75. If three,
          # 0.875. Four: 0.9375. And so forth.
          score do |candidate|
            1.0 - 2.0**(-dataset.where(email: candidate).count)
          end
        end
      end

      field :address do
        # We can canonicalize at both the field level...
        canonicalize do |candidate|
          candidate.upcase.gsub(/\s+/, '')
        end

        source :social_media_accounts do
          canonicalize do |candidate|
            "#{candidiate[:street]}\n#{candidate[:city]}, #{candidate[:state]} #{candidate[:zip]}"
          end

          score do |canonical, equivalencies|
            # Left as an exercise for the reader
          end
        end

        # Relevant sources with their own candidates, scores and transforms
        # would be defined here.
      end
    end
  end
```

Now, to use the inferrer:

```ruby
  person   = Person.where(first_name: 'John', last_name: 'Smith', zip: 55082).first
  inferrer = PersonAttributeInferrer.new(person)

  # If you just want one value, use #best_value_for(field_name). This will prevent
  # the inferrer from calculating scores for and choosing the best value for every
  # field.
  best_value_for(:address)
  # => '123 E 4th St, Stillwater, MN 55082'

  # If you want the inferrer to calculate everything, call #field_values
  inferrer.field_values
  # => {
  #      phone: '(651) 555-5555',
  #      email: 'john.smith@example.com',
  #      address: '123 E 4th St, Stillwater, MN 55082'
  #    }

  # If you want to investigate how a given field's scores were calculated, you can
  # use #evaluator_for(field_name). This responds to #scores, #grouped_scores,
  # #ungrouped_scores, #sourced_weighted_scores and #sourced_unweighted_scores.
  inferrer.evaluator_for(:address)

  # In addition, if you want to investigate how scors were calculated for a given
  # source for a given field, you can get the source evaluator via the field
  # evaluator with #evaluator_for(source_name). This responds to #scores, #candidates,
  # and #raw_candidates.
  inferrer.evaluator_for(:address).evaluator_for(:social_media_accounts)
```

See also `inferrer.scores_for(field_name)`
=end

module AttributeInferrer
  def self.included(klass)
    klass.extend(ClassMethods)
  end

  def dataset_names
    self.class.datasets.keys
  end

  def field_names
    self.class.fields.keys
  end

  def helper_names
    self.class.helpers.keys
  end

  def evaluator_for(field_name)
    @evaluators ||= {}
    @evaluators[field_name] ||= self.class.fields[field_name].evaluator_for(self)
  end

  def field_values
    @field_values ||= field_names.each_with_object({}) do |field_name, memo|
      memo[field_name] = best_value_for(field_name)
    end
  end

  def best_value_for(field_name)
    evaluator_for(field_name).best_value
  end

  def scores_for(field_name)
    evaluator_for(field_name).scores
  end

  module ClassMethods
    def infers(&block)
      instance_exec(&block)
    end

    # DSL helper method to specify a dataset which can be used as a
    # baseline of data for a given source.
    # - key: The name of the dataset. Subsequently used in calls to
    #     #source in field definitions.
    # - block: A block which when called within the context of an
    #     instance of the inferrer (using only those methods defined
    #     as delegates, see #infers above) yields a dataset.
    def dataset(key, &block)
      datasets[key] ||= block
    end

    # DSL helper method to specify a field whose value can be inferred.
    # The block should be a set of calls to #source, which will define
    # sources from which the field's value can be inferred.
    def field(name, &block)
      if fields[name]
        fields[name].instance_exec(&block)
      else
        fields[name] = Field.new(self, name, &block)
      end
    end

    def helper(name, &block)
      helpers[name] = block
    end

    def share(*names)
      names.each do |name|
        helpers[name] = ->(){ @instance.send(name) }
      end
    end

    def datasets
      @datasets ||= {}
    end

    def fields
      @fields ||= {}
    end

    def helpers
      @helpers ||= {}
    end
  end

  class Field
    class InvalidWeightException < Exception; end

    DEFAULT_CANONICALIZER = ->(candidate) { candidate }
    DEFAULT_CHOOSER       = ->(candidate, _equivalencies) { candidate }

    attr_reader :registry, :name, :sources, :weights, :canonicalizer, :chooser

    # registry: the class of the inferrer
    # name: the name of the field whose value can be inferred
    # block: DSL code used to construct this field's sources
    def initialize(registry, name, &block)
      @registry       = registry
      @name           = name
      @canonicalizer  = DEFAULT_CANONICALIZER
      @chooser        = DEFAULT_CHOOSER
      instance_exec(&block)
    end

    def source(dataset_name, params, &block)
      # TODO: Weight as a block, default weight
      if dataset_name && params[:weight] && params[:weight].is_a?(Numeric)
        @sources ||= {}
        @sources[dataset_name] ||= Source.new(dataset_name, self, &block)

        @weights ||= {}
        @weights[dataset_name] ||= params[:weight]
      else
        fail InvalidWeightException('Fields must have a numeric, positive weight')
      end
    end

    def canonicalize(&block)
      if block_given?
        @canonicalizer = block
      else
        @canonicalizer
      end
    end

    def prefer(&block)
      if block_given?
        @chooser = block
      else
        @chooser
      end
    end

    def evaluator_for(instance)
      evaluator_class.new(self, instance)
    end

    def source_names
      @sources.keys
    end

    private

    def helpers
      @registry.helpers
    end

    def evaluator_class
      return @evaluator_class if defined? @evaluator_class

      @evaluator_class = Class.new do
        def initialize(field, instance)
          @field = field
          @instance = instance
        end

        def evaluator_for(source_name)
          @evaluators ||= {}
          @evaluators[source_name] ||= @field.sources[source_name].evaluator_for(@instance)
        end

        def sources
          @evaluators
        end

        def best_value
          return @best_value if defined? @best_value
          @best_value = scores.keys.max_by {|value| scores[value]}
        end

        # yields a hash
        #   key: preferred candidate value
        # value: total score for that value
        def scores
          @scores ||=
          grouped_scores.each_with_object({}) do |(canonical_candidate, hashes), memo|
            key = choose(canonical_candidate, hashes.map { |hash| hash[:candidate] })
            memo[key] = hashes.inject(0.0) do |accumulator, hash|
              accumulator + (hash[:score] || 0.0)
            end
          end
        end

        # yields a hash
        #   key: canonicalized candidate
        # value: array of hashes (candidate, score)
        def grouped_scores
          @grouped_scores ||=
          ungrouped_scores.each_with_object({}) do |(candidate, score), memo|
            key = canonicalize(candidate)
            memo[key] ||= []
            memo[key] << { candidate: candidate, score: score }
          end
        end

        # yields a hash
        #   key: candidate value
        # value: summed candidate score
        def ungrouped_scores
          @ungrouped_scores ||=
          sourced_weighted_scores.each_with_object({}) do |(_source_name, scorecard), memo|
            h = scorecard.each do |(candidate, score)|
              memo[candidate] ||= 0.0
              memo[candidate]  += score
            end
          end
        end

        # yields a nested hash
        #   outer key: source name
        #   inner key: candidate
        #   value    : weighted score
        def sourced_weighted_scores
          @sourced_weighted_scores ||=
          sourced_unweighted_scores.each_with_object({}) do |(source_name, scorecard), memo1|
            memo1[source_name] = scorecard.each_with_object({}) do |(candidate, score), memo2|
              memo2[candidate] = weights[source_name] * score
            end
          end
        end

        # yields a nested hash
        #  outer key: source name
        #  inner key: candidate
        #  value    : raw score
        def sourced_unweighted_scores
          @sourced_unweighted_scores ||=
          @field.source_names.each_with_object({}) do |source_name, memo|
            memo[source_name] = evaluator_for(source_name).scores
          end
        end

        def choose(candidate, equivalencies)
          instance_exec(candidate, equivalencies, &chooser_proc)
        end

        def canonicalize(candidate)
          instance_exec(candidate, &canonicalization_proc)
        end

        def canonicalization_proc
          @field.canonicalize
        end

        def chooser_proc
          @field.prefer
        end

        def weights
          @field.weights
        end

        def respond_to?(m, include_private = true)
          @instance.respond_to?(m, include_private) || super
        end
      end

      helpers.each do |name, block|
        @evaluator_class.send(:define_method, name, &block)
      end

      @evaluator_class
    end
  end

  class Source
    attr_reader :dataset_name, :field

    def initialize(dataset_name, field, &block)
      @dataset_name  = dataset_name
      @field         = field
      @canonicalizer = field.canonicalizer
      @chooser       = field.chooser
      instance_exec(&block)
    end

    def candidates(&block)
      if block_given?
        @candidates = block
      else
        @candidates
      end
    end

    def canonicalize(&block)
      if block_given?
        @canonicalizer = block
      else
        @canonicalizer
      end
    end

    def prefer(&block)
      if block_given?
        @chooser = block
      else
        @chooser
      end
    end

    def score(&block)
      if block_given?
        @score = block
      else
        @score
      end
    end

    def evaluator_for(instance)
      evaluator_class.new(self, instance)
    end

    private

    def helpers
      @helpers ||= @field.registry.helpers
    end

    def evaluator_class
      return @evaluator_class if defined? @evaluator_class

      @evaluator_class = Class.new do
        def initialize(source, instance)
          @source = source
          @instance = instance
        end

        def dataset
          @dataset ||= instance_exec(&dataset_proc)
        end

        def score_for(candidate, equivalencies)
          instance_exec candidate, equivalencies, &score_proc
        end

        def scores
          @scores ||= candidates.each_with_object({}) do |(candidate, equivalencies), memo|
            memo[candidate] = score_for(candidate, equivalencies)
          end
        end

        def candidates
          @candidates ||= raw_candidates.each_with_object({}) do |(candidate, equivalencies), memo|
            memo[choose(candidate, equivalencies)] = equivalencies
          end
        end

        def raw_candidates
          @raw_candidates ||= instance_exec(&candidates_proc).each_with_object({}) do |candidate, memo|
            key = canonicalize(candidate)
            memo[key] ||= []
            memo[key] << candidate
          end
        end

        def choose(candidate, equivalencies)
          instance_exec(candidate, equivalencies, &chooser_proc)
        end

        def canonicalize(candidate)
          instance_exec(candidate, &canonicalization_proc)
        end

        def respond_to?(m, include_private = true)
          @instance.respond_to?(m, include_private) || super
        end

        private

        def dataset_proc
          @source.field.registry.datasets[@source.dataset_name]
        end

        def candidates_proc
          @source.candidates
        end

        def score_proc
          @source.score
        end

        def canonicalization_proc
          @source.canonicalize
        end

        def chooser_proc
          @source.prefer
        end
      end

      helpers.each do |name, block|
        @evaluator_class.send(:define_method, name, &block)
      end

      @evaluator_class
    end
  end
end

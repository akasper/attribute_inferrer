require '../attribute_inferrer'
# Constants
require './indexer/properties/property/raw_mappings'
# Specialized logic for inferring the best title
require './indexer/properties/property/title_analyzer'

# A class for assembling a REscour property -- inferring the best values for each field -- based on multiple input
# sources, using a weighted algorithm
class Property::Inferrer
  include AttributeInferrer

  attr_reader :property

  def initialize(property)
    @property = property
  end

  infers do
    dataset :listings do
      property[:curated_listings]
    end

    dataset :rent_jungle_rents do
      property[:rent_jungle_rents]
    end

    dataset :realty_trac_assessments do
      property[:realty_trac_assessments]
        .where(Sequel.~(SA_APPRAISE_YR: nil))
        .where(USE_CODE_STD: Indexer::Properties::Property::RawMappings::PROPERTY_TYPES.keys)
    end

    dataset :assessment do
      property.assessment
    end

    field :lot_size do # in acres
      # round candidate values to the nearest 10th of an acre
      canonicalize { |candidate| candidate.round(1) }

      source :listings, weight: 0.60 do
        # candidate values are in acres
        candidates { select_distinct_and_map(:acres).reject { |n| n < 0.01 } }

        canonicalize { |candidate| candidate.round(1) }

        prefer do |_canonicalized_candidate, equivalencies|
          equivalencies.max_by do |candidate|
            count_where acres: rangify(candidate)
          end.acres.to_f.round(1)
        end

        score do |_candidate, equivalencies|
          clause = query_for_field_with_multiple_ranges(:acres, rangify(equivalencies))
          geometric_mean_of(
            score_for_count(count_where clause),
            score_for_recency(recency_of most_recent(:updated_at, where: clause))
          )
        end
      end

      source :assessment, weight: 0.40 do
        # candidate values are in square feet
        candidates { (dataset.present? && dataset.lot_size.present?) ? [dataset.lot_size.sqft.to(:acres).to_f] : [] }

        score do |_candidate, _equivalencies|
          score_for_recency(recency_of Date.new(dataset.year || 1900))
        end
      end
    end

    field :management do
      canonicalize { |candidate| canonicalize_name(candidate) }

      prefer do |canonicalized_candidate, equivalencies|
        best_string_among(canonicalized_candidate, equivalencies)
      end

      source :rent_jungle_rents, weight: 1.0 do
        candidates do
          property[:rjr_managements]
            .select(:Management)
            .distinct
            .map { |record| record[:Management] }
        end

        score do |_candidate, equivalencies|
          query = property[:rjr_managements].where(Management: equivalencies)
          geometric_mean_of(
            score_for_count(query.sum(:record_count)),
            score_for_recency(recency_of query.max(:most_recent))
          )
        end
      end
    end

    field :title do
      canonicalize { |candidate| canonicalize_name(candidate) }

      prefer do |canonicalized_candidate, equivalencies|
        best_string_among(canonicalized_candidate, equivalencies)
      end

      source :listings, weight: 0.65 do
        candidates do
          select_distinct_and_map(:title)
            .reject { |candidate| unacceptable_title? candidate }
        end

        score do |_candidate, equivalencies|
          geometric_mean_of(
            score_for_count(distinct_count_for :source_url, where: { title: equivalencies }),
            score_for_recency(recency_of most_recent(:updated_at, where: { title: equivalencies }))
          )
        end
      end

      source :rent_jungle_rents, weight: 0.35 do
        candidates do
          property[:rjr_names]
            .select(:Name)
            .distinct
            .map { |record| record[:Name] }
            .reject { |candidate| unacceptable_title? candidate }
        end

        score do |_candidate, equivalencies|
          query = property[:rjr_names].where(Name: equivalencies)
          geometric_mean_of(
            score_for_count(query.sum(:record_count)),
            score_for_recency(recency_of query.max(:most_recent))
          )
        end
      end
    end

    field :units do
      canonicalize do |candidate|
        if candidate < 50
          candidate
        elsif candidate < 100
          5 * (candidate / 5.0).to_i
        else
          10 * (candidate / 10.0).to_i
        end
      end

      source :assessment, weight: 0.50 do
        candidates { (dataset.present? && dataset.units.present?) ? [dataset.units] : [] }

        score do |_candidate, _equivalencies|
          score_for_recency(recency_of Date.new(dataset.year || 1900))
        end
      end

      source :rent_jungle_rents, weight: 0.50 do
        candidates do
          property[:rjr_units]
            .select(:Units)
            .distinct
            .map { |record| record[:Units] }
        end

        prefer do |_canonicalized_candidate, equivalencies|
          query = property[:rjr_units].where(Units: equivalencies)

          equivalencies.max_by do |_candidate|
            query.sum(:record_count)
          end
        end

        score do |_candidate, equivalencies|
          query = property[:rjr_units].where(Units: equivalencies)

          geometric_mean_of(
            score_for_count(query.sum(:record_count)),
            score_for_recency(recency_of query.max(:most_recent))
          )
        end
      end
    end

    helper :rangify do |arg, margin = 0.00001|
      if arg.respond_to?(:map)
        arg.map { |val| rangify(val, margin) }
      else
        ((arg - margin)..(arg + margin))
      end
    end

    helper :query_for_field_with_multiple_ranges do |field, ranges|
      Sequel.|(*(ranges.map { |range| { field => range } }))
    end

    helper :geometric_mean_of do |*args|
      args.inject(1.0) do |arg, memo|
        memo * (arg < 0.001 || arg.nil? ? 0.001 : arg)
      end**(1.0 / args.length)
    end

    helper :canonicalize_name do |string|
      clean_string(string).titleize
    end

    helper :clean_string do |string|
      string
        .gsub(/^[\s\W]+|[\s\W]+$/, '')  # trim leading and trailing non-words and spaces
        .gsub(/[^\w,\-\.\&\s]+/, ' ')   # convert most non-word characters to spaces
        .gsub(/[^\w\s]{2,}/) { |s| s[0] } # convert consecutive non-whitespace, non-word to the first character
        .gsub(/[ \t]+/, ' ') # convert multiple tabs or spaces to a single space
    end

    helper :score_string do |string|
      s = 1.0
      # Smells:
      # * Canonicalizing this string makes it a significantly different string
      s *= 0.95**(Levenshtein.distance(string.upcase, canonicalize_name(string)))
      # * This string has more than two non-word characters
      s *= 0.90**([0, string.scan(/\W+/).size - 2].max)
      # * This string has more than four capital letters
      s *= 0.90**([0, string.scan(/[A-Z]/).size - 4].max)
      # * This string has multiple consecutive whitespace characters
      s *= 0.80**(string.scan(/\s{2,}/).size)
      # * Really bad sign if the string starts with a non-word character
      s *= 0.10 if string =~ /^\W+.*/
      s
    end

    helper :unacceptable_title? do |candidate|
      !Indexer::Properties::Property::TitleAnalyzer.instance.acceptable?(candidate)
    end

    helper :score_for_count do |count, k = 0.95|
      if count && count >= 1
        1 - k**((count)**0.50)
      else
        0.0001
      end
    end

    helper :score_for_recency do |recency, k = 0.95|
      if recency && recency >= 0
        k**([0, recency - 4].max**(1.0 / 2.0))
      else
        0.0001
      end
    end

    helper :recency_of do |datetime|
      ((Time.zone.today - datetime.to_date).to_f / 7.0).floor.to_f
    end

    helper :select_distinct_and_map do |field|
      dataset
        .where(Sequel.~(field => nil))
        .select(field)
        .distinct
        .map { |d| d[field] }
    end

    helper :count_where do |params|
      dataset.where(params).count
    end

    helper :distinct_count_for do |field, params = {}|
      (params[:where] ? dataset.where(params[:where]) : dataset)
        .distinct
        .count(field)
    end

    helper :most_recent do |datetime_field, params = {}|
      (params[:where] ? dataset.where(params[:where]) : dataset)
        .order(Sequel.desc(datetime_field))
        .first[datetime_field]
    end

    helper :best_string_among do |canonicalized, equivalencies|
      best = clean_string(equivalencies.max_by do |candidate|
        score_string(candidate)
      end)

      score_string(best) < 0.333 ? canonicalized : best
    end

    share :property
  end
end

What is it?
===========

A script to split the CDR database into smaller databases

How does it work?
=================

    run = ->

It queries the CDR database in batches, and distributes each item into a target bin (database) for a given year.

      year = cfg.year
      console.log "#{pkg.name} #{pkg.version} starting for year #{year}."

      limit = 200

      class Saver
        constructor: (@name) ->
          @db = new PouchDB "#{cfg.targets}/cdrs-#{name}", ajax: cfg.ajax
          @queue = []

        push: (doc,seq) ->
          delete doc._rev
          v = doc.variables
          doc._id = "#{v.start_stamp} #{v.ccnq_account} #{v.ccnq_from_e164} #{v.ccnq_to_e164} #{v.billsec}"
          @queue.push doc
          if @queue.length > limit
            @flush seq
          return

        flush: (seq) ->
          return if @only_one_at_a_time
          @only_one_at_a_time = true
          my_queue = @queue
          @queue = []
          successes = 0
          console.log "Submitting #{my_queue.length} entries at seq #{seq ? 'unknown'}."
          @db.bulkDocs my_queue
          .catch (error) ->
            console.log "Oops, #{error} submitting queue."
            []
          .then (responses) ->
            for response, i in responses
              do (response,i) ->
                if response.ok
                  # console.log "Saved #{response.id} for #{@name}"
                  my_queue[i] = null
                  successes++
                else
                  console.log "Oops, #{response.id} failed: #{response}, requeuing it."

Requeue any failed record.

          .then =>
            console.log "Only #{successes} successes out of #{my_queue.length} in this batch."
            my_queue = my_queue.filter (x) -> x?
            @queue = @queue.concat my_queue
            console.log "Next round will start with #{@queue.length} left over."
            @only_one_at_a_time = false
            return
          return

      source = new PouchDB cfg.source, ajax: cfg.ajax
      savers = {}

      changes = source.changes
        include_docs: true
        batch_size: Math.floor limit/4
        since: cfg.since
      .on 'create', ({seq,doc}) ->
        target_month = doc?.variables?.start_stamp?.substr(0,7)
        if target_month? and target_month.substr(0,4) is year
          target = savers[target_month] ?= new Saver target_month
          target.push doc, seq
        else
          console.log "Skipped #{doc._id}"
      .on 'error', (error) ->
        console.log "changes error: #{error}"
      .on 'complete', ->
        console.log "changes complete"
        for k,saver of savers
          do (saver) ->
            saver.flush()

    pkg = require './package.json'
    cfg = require './config.json'
    PouchDB = require 'pouchdb'

    do run

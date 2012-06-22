# -*- ruby -*-

require 'rubygems'
gem 'hoe', '>= 2.1.0'
require 'hoe'
require "rspec/core/rake_task"

# Hoe.plugin :compiler
# Hoe.plugin :gem_prelude_sucks
# Hoe.plugin :inline
# Hoe.plugin :racc
# Hoe.plugin :rubyforge
Hoe.plugin :git
Hoe.plugin :gemspec
Hoe.plugin :bundler
Hoe.plugin :gemcutter
Hoe.plugins.delete :rubyforge

Hoe.spec 'rredis' do
  developer('Dominik Sander', 'git@dsander.de')
  self.description ="RReDis - a round robin database backed by redis" 
  self.summary = "RReDis - dead simple monitoring and graphing round robin database using redis"

  self.readme_file = 'README.md'
  self.history_file = 'CHANGELOG.md'
  self.extra_deps << ["redis"]
end

task :prerelease => [:clobber, :check_manifest, :test]


task :default => :spec
task :test => :spec


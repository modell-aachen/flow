defmodule Ariadne.Flow.Store.Speedrun.Courses do
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.Event
  alias Ariadne.Flow.Store.Speedrun.ReportingStore, as: Store
  alias Ecto.UUID

  def description do
    "Defines a new course and subscribes 10 new students to that course."
  end

  def initial_events_count, do: 5_000_000

  def fill_events_stream do
    (&generate_fill_events/0)
    |> Stream.repeatedly()
    |> Stream.flat_map(& &1)
  end

  defp generate_fill_events do
    course_defined_events =
      Enum.map(1..10, fn _ ->
        course_defined_event(UUID.generate())
      end)

    student_subscribed_events =
      Enum.flat_map(course_defined_events, fn %{data: %{"course_id" => course_id}} ->
        Enum.map(1..10, fn _ ->
          student_subscribed_to_course_event(UUID.generate(), course_id)
        end)
      end)

    course_defined_events ++ student_subscribed_events
  end

  def run_iteration(store) do
    course_id = UUID.generate()
    define_course(store, course_id)

    Enum.each(1..10, fn _ ->
      student_id = UUID.generate()
      subscribe_student_to_course(store, student_id, course_id)
    end)
  end

  defp define_course(store, course_id) do
    query = [course_exists_query_item(course_id)]
    %{events: [] = events} = Store.read(store, query)

    {:ok, _} =
      Store.append(store, course_defined_event(course_id),
        condition: AppendCondition.for_read(query, events)
      )
  end

  defp subscribe_student_to_course(store, student_id, course_id) do
    query = [
      course_exists_query_item(course_id),
      course_capacity_query_item(course_id),
      student_already_subscribed_query_item(student_id, course_id),
      number_of_course_subscriptions_query_item(course_id),
      number_of_student_subscription_query_item(student_id)
    ]

    %{events: events} = Store.read(store, query)

    {:ok, _} =
      Store.append(store, student_subscribed_to_course_event(student_id, course_id),
        condition: AppendCondition.for_read(query, events)
      )
  end

  defp course_exists_query_item(course_id) do
    %{types: ["CourseDefined"], tags: ["course:#{course_id}"]}
  end

  defp course_capacity_query_item(course_id) do
    %{types: ["CourseDefined", "CourseCapacityChanged"], tags: ["course:#{course_id}"]}
  end

  defp student_already_subscribed_query_item(student_id, course_id) do
    %{
      types: ["StudentSubscribedToCourse"],
      tags: ["student:#{student_id}", "course:#{course_id}"]
    }
  end

  defp number_of_course_subscriptions_query_item(course_id) do
    %{types: ["StudentSubscribedToCourse"], tags: ["course:#{course_id}"]}
  end

  defp number_of_student_subscription_query_item(student_id) do
    %{types: ["StudentSubscribedToCourse"], tags: ["student:#{student_id}"]}
  end

  defp course_defined_event(course_id) do
    %Event{
      type: "CourseDefined",
      data: %{"course_id" => course_id, "capacity" => 10},
      tags: ["course:#{course_id}"]
    }
  end

  defp student_subscribed_to_course_event(student_id, course_id) do
    %Event{
      type: "StudentSubscribedToCourse",
      data: %{"student_id" => student_id, "course_id" => course_id},
      tags: ["student:#{student_id}", "course:#{course_id}"]
    }
  end
end
